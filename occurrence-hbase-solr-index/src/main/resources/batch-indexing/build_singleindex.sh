export LIB_JARS=`ls -dm lib/* | tr -d '[[:space:]]'`
export HADOOP_CLIENT_OPTS="-Xmx3276m $HADOOP_CLIENT_OPTS"
export HADOOP_CLASSPATH=lib/*:/opt/cloudera/parcels/CDH/lib/hbase-solr/lib/*:/opt/cloudera/parcels/CDH/lib/hbase/*:/opt/cloudera/parcels/CDH/lib/solr/contrib/mr/*:/opt/cloudera/parcels/CDH/lib/search/lib/*:/opt/cloudera/parcels/CDH/lib/solr/contrib/mr/*
hadoop jar /opt/cloudera/parcels/CDH/lib/hbase-solr/tools/hbase-indexer-mr-*-job.jar --conf /etc/hbase/conf/hbase-site.xml -D 'mapreduce.reduce.shuffle.input.buffer.percent=0.2' -D 'mapreduce.reduce.shuffle.parallelcopies=5' -D 'mapreduce.map.memory.mb=4096' -D 'mapreduce.map.java.opts=-Xmx3584m' -D 'mapreduce.reduce.memory.mb=8192' -D 'mapreduce.reduce.java.opts=-Xmx7680m' -libjars $LIB_JARS,/opt/cloudera/parcels/CDH/lib/solr/contrib/mr/search-mr.jar,/opt/cloudera/parcels/CDH/lib/hbase/hbase-server-1.0.0-cdh5.4.2.jar,/opt/cloudera/parcels/CDH/lib/hbase/hbase-common-1.0.0-cdh5.4.2.jar,/opt/cloudera/parcels/CDH/lib/hbase/hbase-common-1.0.0-cdh5.4.2.jar,/opt/cloudera/parcels/CDH/lib/hbase-solr/lib/hbase-indexer-engine-1.5-cdh5.4.2.jar,/opt/cloudera/parcels/CDH/lib/hbase-solr/lib/hbase-indexer-common-1.5-cdh5.4.2.jar,/opt/cloudera/parcels/CDH/lib/hbase-solr/lib/hbase-client.jar,/opt/cloudera/parcels/CDH/lib/hbase-solr/lib/hbase-sep-impl-1.5-hbase1.0-cdh5.4.2.jar,/opt/cloudera/parcels/CDH/lib/hbase-solr/lib/hbase-protocol.jar,/opt/cloudera/parcels/CDH/lib/hbase-solr/lib/hbase-hadoop2-compat.jar,/opt/cloudera/parcels/CDH/lib/hbase-solr/lib/htrace-core-3.1.0-incubating.jar,/opt/cloudera/parcels/CDH/lib/hbase-solr/lib/noggit-0.5.jar,/opt/cloudera/parcels/CDH/lib/solr/solr-solrj-4.10.3-cdh5.4.2.jar,/opt/cloudera/parcels/CDH/lib/solr/solr-core.jar,/opt/cloudera/parcels/CDH/lib/solr/solr-security-util.jar,/opt/cloudera/parcels/CDH/lib/hbase-solr/lib/lucene-analyzers-common.jar,/opt/cloudera/parcels/CDH/lib/hbase-solr/lib/lucene-queries.jar,/opt/cloudera/parcels/CDH/lib/hbase-solr/lib/lucene-spatial.jar,/opt/cloudera/parcels/CDH/lib/hbase-solr/lib/,/opt/cloudera/parcels/CDH/lib/hbase-solr/lib/lucene-core.jar,/opt/cloudera/parcels/CDH/lib/hbase-solr/lib/lucene-codecs.jar,/opt/cloudera/parcels/CDH/lib/hbase-solr/lib/lucene-suggest.jar,/opt/cloudera/parcels/CDH/lib/hbase-solr/lib/lucene-join.jar,/opt/cloudera/parcels/CDH/lib/hbase-solr/lib/spatial4j-0.4.1.jar,/opt/cloudera/parcels/CDH/lib/hbase-solr/lib/lucene-highlighter.jar,/opt/cloudera/parcels/CDH/lib/hbase-solr/lib/commons-fileupload-1.2.1.jar,/opt/cloudera/parcels/CDH/lib/hbase-solr/lib/metrics-core-2.2.0.jar,/opt/cloudera/parcels/CDH/lib/hbase-solr/lib/metrics-healthchecks-3.0.2.jar,/opt/cloudera/parcels/CDH/lib/search/lib/kite-morphlines-core.jar,/opt/cloudera/parcels/CDH/lib/hbase-solr/lib/hbase-indexer-morphlines-1.5-cdh5.4.2.jar,/opt/cloudera/parcels/CDH/lib/hbase-solr/lib/config-1.0.2.jar,/opt/cloudera/parcels/CDH/lib/hbase-solr/lib/org.restlet-2.1.1.jar --hbase-indexer-file hbase_occurrence_batch_morphline.xml --shards 1 --log4j conf/log4j.properties --output-dir $1 --solr-home-dir ../solr/ --verbose --overwrite-output-dir