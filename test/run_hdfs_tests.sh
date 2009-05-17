#!/bin/sh

HADOOP_ROOT=/opt/hadoop
export CLASSPATH=\
${HADOOP_ROOT}/lib/xmlenc-0.52.jar:\
${HADOOP_ROOT}/lib/slf4j-log4j12-1.4.3.jar:\
${HADOOP_ROOT}/lib/slf4j-api-1.4.3.jar:\
${HADOOP_ROOT}/lib/servlet-api.jar:\
${HADOOP_ROOT}/lib/oro-2.0.8.jar:\
${HADOOP_ROOT}/lib/log4j-1.2.15.jar:\
${HADOOP_ROOT}/lib/kfs-0.2.0.jar:\
${HADOOP_ROOT}/lib/junit-3.8.1.jar:\
${HADOOP_ROOT}/lib/jetty-5.1.4.jar:\
${HADOOP_ROOT}/lib/jets3t-0.6.1.jar:\
${HADOOP_ROOT}/lib/hsqldb-1.8.0.10.jar:\
${HADOOP_ROOT}/lib/commons-net-1.4.1.jar:\
${HADOOP_ROOT}/lib/commons-logging-api-1.0.4.jar:\
${HADOOP_ROOT}/lib/commons-logging-1.0.4.jar:\
${HADOOP_ROOT}/lib/commons-httpclient-3.0.1.jar:\
${HADOOP_ROOT}/lib/commons-codec-1.3.jar:\
${HADOOP_ROOT}/lib/commons-cli-2.0-SNAPSHOT.jar:\
${HADOOP_ROOT}/hadoop-0.19.1-core.jar:\
${HADOOP_ROOT}/hadoop-0.19.1-tools.jar:\
${HADOOP_ROOT}/conf

export LD_LIBRARY_PATH=../src/libhdfs:${LD_LIBRARY_PATH}

python test_hdfs_plain_disk.py
python test_hdfs_default.py
python test_hdfs_localhost.py
