<#--
  This is a freemarker template which will generate an HQL script which is run at download time.
  When run in Hive as a parameterized query, this will create a set of tables ...
  TODO: document when we actually know something accurate to write here...
-->
<#-- Required syntax to escape Hive parameters. Outputs "USE ${hiveDB};" -->
USE ${r"${hiveDB}"};

-- setup for our custom, combinable deflated compression
SET hive.exec.compress.output=true;
SET io.seqfile.compression.type=BLOCK;
SET mapred.output.compression.codec=org.gbif.hadoop.compress.d2.D2Codec;
SET io.compression.codecs=org.gbif.hadoop.compress.d2.D2Codec;
CREATE TEMPORARY FUNCTION toISO8601 AS 'org.gbif.occurrence.hive.udf.ToISO8601UDF';
CREATE TEMPORARY FUNCTION contains AS 'org.gbif.occurrence.hive.udf.ContainsUDF';
CREATE TEMPORARY FUNCTION joinArray AS 'brickhouse.udf.collect.JoinArrayUDF';

ADD JAR hdfs://ha-nn/occurrence-download-workflows-devpipelines/lib/elasticsearch-hadoop-5.6.2.jar;
-- in case this job is relaunched
DROP TABLE IF EXISTS ${r"${occurrenceTable}"};
DROP TABLE IF EXISTS ${r"${occurrenceTable}"}_citation;

CREATE EXTERNAL TABLE ${r"${occurrenceTable}"}_es ( basisOfRecord STRING,class STRING,classKey BIGINT, day BIGINT, decimalLatitude FLOAT, decimalLongitude FLOAT, country STRING, family STRING, familyKey BIGINT, genus STRING, genusKey BIGINT, geodeticDatum STRING, infraspecificEpithet STRING, kingdom STRING, kingdomKey BIGINT, month BIGINT, nubKey BIGINT, occurrenceId STRING, order STRING, orderKey BIGINT, phylum STRING, phylumKey BIGINT, scientificName STRING, scientificNameAuthorship STRING, species STRING, speciesKey BIGINT, specificEpithet STRING, taxonRank STRING, year BIGINT)
STORED BY 'org.elasticsearch.hadoop.hive.EsStorageHandler' TBLPROPERTIES('es.resource' = 'occurrence/occurrence'
, 'es.query' = '${r"${esQuery}"}','es.node'='c3n1.gbif.org,c3n2.gbif.org,c3n3.gbif.org');


-- pre-create verbatim table so it can be used in the multi-insert
CREATE TABLE ${r"${occurrenceTable}"} ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
TBLPROPERTIES ("serialization.null.format"="")
AS SELECT * FROM ${r"${occurrenceTable}"}_es;

-- creates the citations table, citation table is not compressed since it is read later from Java as TSV.
SET mapred.output.compress=false;
SET hive.exec.compress.output=false;
CREATE TABLE ${r"${occurrenceTable}"}_citation ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' AS SELECT datasetkey, count(*) as num_occurrences FROM ${r"${occurrenceTable}"} WHERE country IS NOT NULL GROUP BY datasetkey;
