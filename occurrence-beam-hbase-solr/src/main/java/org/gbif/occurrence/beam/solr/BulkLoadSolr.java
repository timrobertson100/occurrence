package org.gbif.occurrence.beam.solr;

import org.apache.beam.runners.spark.SparkRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.hbase.HBaseIO;
import org.apache.beam.sdk.io.solr.SolrIO;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.solr.common.SolrInputDocument;
import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.occurrence.persistence.util.OccurrenceBuilder;
import org.gbif.occurrence.search.writer.SolrOccurrenceWriter;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Executes a pipeline that reads HBase and loads SOLR. */
public class BulkLoadSolr {

  private static final Logger LOG = LoggerFactory.getLogger(BulkLoadSolr.class);

  public static void main(String[] args) {
    PipelineOptionsFactory.register(BulkLoadOptions.class);
    BulkLoadOptions options = PipelineOptionsFactory.fromArgs(args).as(BulkLoadOptions.class);
    options.setRunner(SparkRunner.class);
    Pipeline p = Pipeline.create(options);

    Counter docsIndexed =  Metrics.counter(BulkLoadSolr.class,"docsIndexed");
    Counter docsFailed =  Metrics.counter(BulkLoadSolr.class,"docsFailed");

    String solrCollection = options.getSolrCollection();

    Configuration hbaseConfig = HBaseConfiguration.create();
    hbaseConfig.set("hbase.zookeeper.quorum", options.getHbaseZk());

    Scan scan = new Scan();
    scan.setBatch(options.getBatchSize()); // for safety
    scan.addFamily("o".getBytes());

    int keyDivisor = options.getKeyDivisor();
    int keyReminder = options.getKeyRemainder();
    String table =  options.getTable();

    PCollection<Result> rows =
        p.apply(
            "read",
            HBaseIO.read().withConfiguration(hbaseConfig).withScan(scan).withTableId(table));

    PCollection<SolrInputDocument> docs =
        rows.apply(
            "convert",
            ParDo.of(
                new DoFn<Result, SolrInputDocument>() {

                  @ProcessElement
                  public void processElement(ProcessContext c) {
                    Result row = c.element();
                    try {
                      Occurrence occurrence = OccurrenceBuilder.buildOccurrence(row);
                      if (occurrence.getKey() % keyDivisor == keyReminder) {
                        SolrInputDocument document = SolrOccurrenceWriter.buildOccSolrDocument(occurrence);
                        c.output(document);
                        docsIndexed.inc();
                      }
                    } catch (Exception e) {
                      // Expected for bad data
                      LOG.info("Error reading HBase record {} ", Bytes.toInt(row.getRow()));
                      docsFailed.inc();
                    }
                  }
                }));

    final SolrIO.ConnectionConfiguration conn = SolrIO.ConnectionConfiguration.create(options.getSolrZk());

    docs.apply("write",
        SolrIO.write()
            .to(solrCollection)
            .withConnectionConfiguration(conn)
            .withRetryConfiguration(
                SolrIO.RetryConfiguration.create(options.getMaxAttempts(), Duration.standardMinutes(1))));

    PipelineResult result = p.run();
    result.waitUntilFinish();
  }
}
