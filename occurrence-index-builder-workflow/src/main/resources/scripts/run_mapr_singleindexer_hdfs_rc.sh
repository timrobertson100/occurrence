export HADOOP_CLIENT_OPTS="-Xmx2048m $HADOOP_CLIENT_OPTS"
export HADOOP_CLASSPATH=$HADOOP_CLASSPATH:jts-1.13.jar
export HADOOP_USER_CLASSPATH_FIRST=true
hadoop --config /etc/hadoop/conf/ jar $SOLR_HOME/dist/solr-map-reduce-*.jar -D 'mapreduce.reduce.shuffle.input.buffer.percent=0.2' -D 'mapreduce.reduce.shuffle.parallelcopies=5' -D 'mapreduce.map.memory.mb=4096' -D 'mapreduce.map.java.opts=-Xmx3584m' -D 'mapreduce.reduce.memory.mb=8192' -D 'mapreduce.reduce.java.opts=-Xmx7680m' -D 'mapreduce.job.user.classpath.first=true' \
-libjars "$HADOOP_LIBJAR,jts-1.13.jar" --morphline-file ../conf/avro_solr_occurrence_morphline.conf \
--zk-host c1n1.gbif.org:2181,c1n2.gbif.org:2181,c1n3.gbif.org:2181/solruat --output-dir hdfs://prodmaster1-vh.gbif.org:8020/solr/uat_occurrence_from_hdfs \
--collection uat_occurrence --log4j log4j.properties \
--verbose "hdfs://prodmaster1-vh.gbif.org:8020/user/hive/warehouse/uat.db/occurrence_avro/" \
--go-live



