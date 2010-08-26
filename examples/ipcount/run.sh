#!/bin/bash

# BEGIN_COPYRIGHT
# END_COPYRIGHT

# You need a working Hadoop cluster to run this.

nargs=1
if [ $# -lt ${nargs} ]; then
    echo "USAGE: $(basename $0) INPUT_DIR"
    exit 2
fi
INPUT_DIR=$1


HADOOP=${HADOOP_HOME}/bin/hadoop

BINDIR=bin

SCRIPT=${BINDIR}/ipcount.py
SCRIPT_BASENAME=$(basename ${SCRIPT})

CONFDIR=conf
CONF=${CONFDIR}/ipcount.xml

INPUT_BASENAME=$(basename ${INPUT_DIR})

WD=test_ipcount
HDFS_SCRIPT=${WD}/bin/${SCRIPT_BASENAME}
CONF_OVERRIDE="-D mapred.map.tasks=2 -D mapred.reduce.tasks=2"

rm -rf output

echo -n "Waiting for HDFS to exit safe mode... "
${HADOOP} dfsadmin -safemode wait
${HADOOP} dfs -rmr ${WD}
${HADOOP} dfs -mkdir ${WD}/bin
${HADOOP} dfs -put ${INPUT_DIR} ${WD}/${INPUT_BASENAME}
${HADOOP} dfs -put ${SCRIPT} ${HDFS_SCRIPT}
${HADOOP} pipes ${CONF_OVERRIDE} -conf ${CONF} -program ${HDFS_SCRIPT} \
    -input ${WD}/${INPUT_BASENAME} -output ${WD}/output

${HADOOP} dfs -cat ${WD}/output/part* | sort -k2,2nr | head -n 5
#${HADOOP} dfs -rmr ${WD}
