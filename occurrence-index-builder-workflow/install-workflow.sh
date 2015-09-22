#exit on any failure
set -e

#!/bin/bash
P=$1
TOKEN=$2

echo "Get latest tables-coord config profiles from github"
curl -s -H "Authorization: token $TOKEN" -H 'Accept: application/vnd.github.v3.raw' -O -L https://api.github.com/repos/gbif/gbif-configuration/contents/occurrence-index-builder/$P.properties

oozie_url=`cat $P.properties| grep "oozie.url" | cut -d'=' -f2`

echo "Assembling jar for $ENV"

mvn -Poozie clean package assembly:single
mvn -Psolr package assembly:single

echo "Copy to hadoop"
hdfs dfs -rm -r /occurrence-index-builder-$P/
hdfs dfs -mkdir /occurrence-index-builder-$P/
hdfs dfs -copyFromLocal target/ /occurrence-index-builder-$P/

echo "${oozie_url}"
oozie job --oozie ${oozie_url} -config $P.properties -run

