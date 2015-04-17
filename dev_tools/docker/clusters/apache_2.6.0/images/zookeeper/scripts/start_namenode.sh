#!/bin/bash

#--- manage_deamon stardard
export HADOOP_LOG_DIR=${HDFS_LOG_DIR}
export HADOOP_PID_DIR=${HDFS_PID_DIR}

python /tmp/zk_wait.py namenode

su ${HDFS_USER} -c "${HADOOP_HOME}/bin/hdfs --config ${HADOOP_CONF_DIR} namenode -format"

# we should actually check that the namenode is up ...
python /tmp/zk_set.py namenode up

su ${HDFS_USER} -c "${HADOOP_HOME}/bin/hdfs --config ${HADOOP_CONF_DIR} namenode"


