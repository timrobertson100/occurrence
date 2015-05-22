#create solr configuration
solrctl instancedir --create dev_occurrence solr/
solrctl collection --create dev_occurrence -s 3 -c dev_occurrence -r 2 -m 1 -n 3
ZK_HOST=$1
#occurrence
SOLR_COLLECTION=$2
export HADOOP_CLASSPATH=lib/*:/opt/cloudera/parcels/CDH/lib/hbase-solr/lib/*
hadoop --config /etc/hadoop/conf jar /opt/cloudera/parcels/CDH/lib/hbase-solr/tools/hbase-indexer-mr-*-job.jar --conf conf/hbase-site.xml -D 'mapred.child.java.opts=-Xmx500m' -libjars lib/ --hbase-indexer-file hbase_occurrence_batch_morphline.xml --zk-host $ZK_HOST --collection $SOLR_COLLECTION --go-live --log4j conf/log4j.properties
