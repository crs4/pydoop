#!/bin/bash

#--- manage_deamon stardard
export HADOOP_LOG_DIR=${HDFS_LOG_DIR}
export HADOOP_PID_DIR=${HDFS_PID_DIR}

python /tmp/zk_wait.py datanode

su - ${HDFS_USER} -p -c "${HADOOP_HOME}/sbin/hadoop-daemon.sh --config ${HADOOP_CONF_DIR} start datanode"

# FIXME
python /tmp/zk_set.py datanode up

echo "Log is  ${HDFS_LOG_DIR}/*datanode-${HOSTNAME}.out"

tail -f ${HDFS_LOG_DIR}/*datanode-${HOSTNAME}.out



