ZK_HOST=$1
#occurrence
SOLR_COLLECTION=$2

#delete collection and instancedir if exist
set -e
solrctl --zk $ZK_HOST collection --delete $SOLR_COLLECTION || true
solrctl --zk $ZK_HOST instancedir --delete $SOLR_COLLECTION  ||true

set +e
#create solr configuration
solrctl --zk $ZK_HOST instancedir --create $SOLR_COLLECTION solr/
solrctl --zk $ZK_HOST collection --create $SOLR_COLLECTION -s 3 -c $SOLR_COLLECTION -r 2 -m 2
export HADOOP_CLASSPATH=lib/*:/opt/cloudera/parcels/CDH/lib/hbase-solr/lib/*:/opt/cloudera/parcels/CDH/lib/solr/contrib/mr/*
hadoop --config /etc/hadoop/conf jar /opt/cloudera/parcels/CDH/lib/hbase-solr/tools/hbase-indexer-mr-*-job.jar --conf conf/hbase-site.xml -D 'mapred.child.java.opts=-Xmx500m' -libjars lib/ --hbase-indexer-file hbase_occurrence_batch_morphline.xml --zk-host $ZK_HOST --collection $SOLR_COLLECTION --go-live --log4j conf/log4j.properties
