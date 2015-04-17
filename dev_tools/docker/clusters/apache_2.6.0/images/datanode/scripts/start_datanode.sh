#!/bin/bash

#--- manage_deamon stardard
export HADOOP_LOG_DIR=${HDFS_LOG_DIR}
export HADOOP_PID_DIR=${HDFS_PID_DIR}

python /tmp/zk_wait.py datanode

# FIXME
python /tmp/zk_set.py datanode up

su ${HDFS_USER} -c "${HADOOP_HOME}/bin/hdfs --config ${HADOOP_CONF_DIR} datanode 2>&1 >/tmp/logs/datanode.out"



