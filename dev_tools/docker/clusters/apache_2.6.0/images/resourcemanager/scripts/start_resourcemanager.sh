#!/bin/bash

export HADOOP_LOG_DIR=${HDFS_LOG_DIR}
export HADOOP_PID_DIR=${HDFS_PID_DIR}


python /tmp/zk_wait.py resourcemanager

# we should actually check that the resourcemanager is up ...
python /tmp/zk_set.py resourcemanager up


su ${YARN_USER} -c "${HADOOP_HOME}/bin/yarn --config ${HADOOP_CONF_DIR} resourcemanager 2>&1 > /tmp/logs/resourcemanager.out"

