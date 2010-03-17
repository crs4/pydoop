#!/bin/bash

# You need a working Hadoop cluster to run this.

nargs=1
if [ $# -ne $nargs ]; then
    echo "Usage: $0 EXAMPLE_SCRIPT_BASENAME (e.g., wordcount-full.py)"
    exit 2
fi
SCRIPT_BASENAME=$1
echo running test for ${SCRIPT_BASENAME}

HADOOP=${HADOOP_HOME}/bin/hadoop

BINDIR=../bin
CONFDIR=../conf

SCRIPT=${BINDIR}/${SCRIPT_BASENAME}
CONF=${CONFDIR}/${SCRIPT_BASENAME/.py/.xml}
WD=test_pydoop_examples
HDFS_SCRIPT=${WD}/bin/${SCRIPT_BASENAME}
CONF_OVERRIDE="-D mapred.map.tasks=2 -D mapred.reduce.tasks=2"

rm -rf output

echo -n "Waiting for HDFS to exit safe mode... "
${HADOOP} dfsadmin -safemode wait
${HADOOP} dfs -mkdir ${WD}/bin
${HADOOP} dfs -put ${SCRIPT} ${HDFS_SCRIPT}
${HADOOP} dfs -put input ${WD}/input
${HADOOP} pipes ${CONF_OVERRIDE} -conf ${CONF} -program ${HDFS_SCRIPT} \
    -input ${WD}/input -output ${WD}/output
${HADOOP} dfs -get ${WD}/output output
${HADOOP} dfs -rmr ${WD}

sort output/part* -o output/global.out
sort java-output/part-r-00000 -o java-output/part-r-00000.sorted
diff --brief java-output/part-r-00000.sorted output/global.out
[[ "$?" == 0 ]] || echo "TEST FAILED!"
