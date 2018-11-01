package org.gbif.occurrence.beam.solr;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import org.apache.beam.runners.spark.SparkRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.hadoop.inputformat.HadoopInputFormatIO;
import org.apache.beam.sdk.io.hbase.HBaseIO;
import org.apache.beam.sdk.io.solr.SolrIO;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableSnapshotInputFormat;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.solr.common.SolrInputDocument;
import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.occurrence.persistence.util.OccurrenceBuilder;
import org.gbif.occurrence.search.writer.SolrOccurrenceWriter;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Executes a pipeline that reads HBase and loads SOLR. */
public class BulkLoadSolr {

  private static final Logger LOG = LoggerFactory.getLogger(BulkLoadSolr.class);

  private static Configuration snapshotConfig(String table, String hbaseZk, Scan scan) {
    try {
      Configuration hbaseConf = HBaseConfiguration.create();
      hbaseConf.set("hbase.zookeeper.quorum", hbaseZk);
      hbaseConf.set("hbase.rootdir", "/hbase");
      hbaseConf.setClass(
        "mapreduce.job.inputformat.class", TableSnapshotInputFormat.class, InputFormat.class);
      hbaseConf.setClass("key.class", ImmutableBytesWritable.class, Writable.class);
      hbaseConf.setClass("value.class", Result.class, Writable.class);
      ClientProtos.Scan proto = ProtobufUtil.toScan(scan);
      hbaseConf.set(TableInputFormat.SCAN, Base64.encodeBytes(proto.toByteArray()));
      // Make use of existing utility methods
      Job job = Job.getInstance(hbaseConf); // creates internal clone of hbaseConf
      TableSnapshotInputFormat.setInput(job, table, new Path("/tmp/snapshot_restore"));
      hbaseConf = job.getConfiguration();
      return hbaseConf;
    } catch (IOException ex) {
      throw Throwables.propagate(ex);
    }
  }

  private static PCollectionTuple fromSnapshot(TupleTag<SolrInputDocument> solrDocs, TupleTag<String> failedRows, BulkLoadOptions options, Pipeline p) {
    Counter docsIndexed =  Metrics.counter(BulkLoadSolr.class,"docsIndexed");
    Counter docsFailed =  Metrics.counter(BulkLoadSolr.class,"docsFailed");

    String snapshotTable =  options.getSnapshotTable();

    Scan scan = new Scan();
    scan.setBatch(options.getBatchSize()); // for safety
    scan.addFamily("o".getBytes());

    Configuration hbaseConf = snapshotConfig(snapshotTable, options.getHbaseZk(), scan);

    PCollection<KV<ImmutableBytesWritable,Result>> rows =
      p.apply("read",
        HadoopInputFormatIO.<ImmutableBytesWritable, Result>read()
          .withConfiguration(hbaseConf));

    return
      rows.apply(
        "convert",
        ParDo.of(
          new DoFn<KV<ImmutableBytesWritable,Result>, SolrInputDocument>() {
            private ObjectMapper mapper = new ObjectMapper();

            private String asJson(Result record) {
              try {
                Map<String,String> recordMap = record.getFamilyMap("o".getBytes()).entrySet()
                  .stream()
                  .collect(Collectors.toMap(entry -> Bytes.toString(entry.getKey()), entry -> Bytes.toString(entry.getValue())));
                recordMap.put("key", Integer.toString(Bytes.toInt(record.getRow())));
                return mapper.writeValueAsString(recordMap);
              } catch (JsonProcessingException ex) {
                LOG.error("Error converting to JSON", ex);
                throw Throwables.propagate(ex);
              }
            }

            @ProcessElement
            public void processElement(ProcessContext c) {
              Result row = c.element().getValue();
              try {
                Occurrence occurrence = OccurrenceBuilder.buildOccurrence(row);
                SolrInputDocument document = SolrOccurrenceWriter.buildOccSolrDocument(occurrence);
                c.output(solrDocs, document);
                docsIndexed.inc();
              } catch (Exception ex) {
                // Expected for bad data
                LOG.error("Error reading HBase record {} ", Bytes.toInt(row.getRow()), ex);
                c.output(failedRows, asJson(row));
                docsFailed.inc();
              }
            }
          }).withOutputTags(solrDocs, TupleTagList.of(failedRows)));


  }

  private static PCollectionTuple fromHBaseTable(TupleTag<SolrInputDocument> solrDocs, TupleTag<String> failedRows, BulkLoadOptions options, Pipeline p) {

    Counter docsIndexed =  Metrics.counter(BulkLoadSolr.class,"docsIndexed");
    Counter docsFailed =  Metrics.counter(BulkLoadSolr.class,"docsFailed");

    Configuration hbaseConf = HBaseConfiguration.create();
    hbaseConf.set("hbase.zookeeper.quorum", options.getHbaseZk());
    String table =  options.getTable();

    Scan scan = new Scan();
    scan.setBatch(options.getBatchSize()); // for safety
    scan.addFamily("o".getBytes());

    PCollection<Result> rows =
      p.apply(
        "read",
        HBaseIO.read().withConfiguration(hbaseConf).withScan(scan).withTableId(table));

    return
      rows.apply(
        "convert",
        ParDo.of(
          new DoFn<Result, SolrInputDocument>() {
            private ObjectMapper mapper = new ObjectMapper();

            private String asJson(Result record) {
              try {
                Map<String,String> recordMap = record.getFamilyMap("o".getBytes()).entrySet()
                  .stream()
                  .collect(Collectors.toMap(entry -> Bytes.toString(entry.getKey()), entry -> Bytes.toString(entry.getValue())));
                recordMap.put("key", Integer.toString(Bytes.toInt(record.getRow())));
                return mapper.writeValueAsString(recordMap);
              } catch (JsonProcessingException ex) {
                LOG.error("Error converting to JSON", ex);
                throw Throwables.propagate(ex);
              }
            }

            @ProcessElement
            public void processElement(ProcessContext c) {
              Result row = c.element();
              try {
                Occurrence occurrence = OccurrenceBuilder.buildOccurrence(row);
                SolrInputDocument document = SolrOccurrenceWriter.buildOccSolrDocument(occurrence);
                c.output(solrDocs, document);
                docsIndexed.inc();
              } catch (Exception ex) {
                // Expected for bad data
                LOG.error("Error reading HBase record {} ", Bytes.toInt(row.getRow()), ex);
                c.output(failedRows, asJson(row));
                docsFailed.inc();
              }
            }
          }).withOutputTags(solrDocs, TupleTagList.of(failedRows)));

  }


  public static void main(String[] args) {
    PipelineOptionsFactory.register(BulkLoadOptions.class);
    BulkLoadOptions options = PipelineOptionsFactory.fromArgs(args).as(BulkLoadOptions.class);
    options.setRunner(SparkRunner.class);
    Pipeline p = Pipeline.create(options);

    String solrCollection = options.getSolrCollection();
    String errorFile = options.getErrorFile();

    Configuration hbaseConf = HBaseConfiguration.create();
    hbaseConf.set("hbase.zookeeper.quorum", options.getHbaseZk());

    Scan scan = new Scan();
    scan.setBatch(options.getBatchSize()); // for safety
    scan.addFamily("o".getBytes());

    TupleTag<SolrInputDocument> solrDocs = new TupleTag<SolrInputDocument>(){};
    TupleTag<String> failedRows  = new TupleTag<String>(){};


    final SolrIO.ConnectionConfiguration conn = SolrIO.ConnectionConfiguration.create(options.getSolrZk());

    PCollectionTuple docs = Objects.nonNull(options.getTable())? fromHBaseTable(solrDocs, failedRows, options, p) : fromSnapshot(solrDocs, failedRows, options, p);

    docs.get(solrDocs)
      .apply("write",
        SolrIO.write()
            .to(solrCollection)
            .withConnectionConfiguration(conn)
            .withRetryConfiguration(
                SolrIO.RetryConfiguration.create(options.getMaxAttempts(), Duration.standardMinutes(1))));
    docs.get(failedRows)
      .apply("export", TextIO.write().to(errorFile).withoutSharding());

    PipelineResult result = p.run();
    result.waitUntilFinish();
  }
}
