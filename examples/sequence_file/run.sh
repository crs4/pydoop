#!/bin/bash

# BEGIN_COPYRIGHT
# END_COPYRIGHT

# You need a working Hadoop cluster to run this.

HADOOP=${HADOOP_HOME}/bin/hadoop

BINDIR=bin

WC_SCRIPT=${BINDIR}/wordcount.py
WC_SCRIPT_BASENAME=$(basename ${WC_SCRIPT})

FILTER_SCRIPT=${BINDIR}/filter.py
FILTER_SCRIPT_BASENAME=$(basename ${FILTER_SCRIPT})

CONFDIR=conf
INPUTDIR=../input
INPUT_BASENAME=$(basename ${INPUTDIR})

#WC_SCRIPT=${BINDIR}/${WC_SCRIPT_BASENAME}  # ?
#FILTER_SCRIPT=${BINDIR}/${FILTER_SCRIPT_BASENAME}  # ?

WC_CONF=${CONFDIR}/wordcount.xml
FILTER_CONF=${CONFDIR}/filter.xml

WD=test_sequence_file
HDFS_WC_SCRIPT=${WD}/bin/${WC_SCRIPT_BASENAME}
HDFS_FILTER_SCRIPT=${WD}/bin/${FILTER_SCRIPT_BASENAME}

CONF_OVERRIDE="-D mapred.map.tasks=2 -D mapred.reduce.tasks=2"

rm -rf output

echo -n "Waiting for HDFS to exit safe mode... "
${HADOOP} dfsadmin -safemode wait
${HADOOP} dfs -rmr ${WD}
${HADOOP} dfs -mkdir ${WD}/bin
${HADOOP} dfs -put ${INPUTDIR} ${WD}/${INPUT_BASENAME}
${HADOOP} dfs -put ${WC_SCRIPT} ${HDFS_WC_SCRIPT}
${HADOOP} dfs -put ${FILTER_SCRIPT} ${HDFS_FILTER_SCRIPT}
${HADOOP} pipes ${CONF_OVERRIDE} -conf ${WC_CONF} -program ${HDFS_WC_SCRIPT} \
    -input ${WD}/${INPUT_BASENAME} -output ${WD}/wc_output
${HADOOP} pipes -conf ${FILTER_CONF} -program ${HDFS_FILTER_SCRIPT} \
    -input ${WD}/wc_output -output ${WD}/filter_output

${HADOOP} dfs -get ${WD}/filter_output output
#${HADOOP} dfs -rmr ${WD}
