#!/bin/bash

export YARN_LOG_DIR=${YARN_LOG_DIR}
export HADOOP_PID_DIR=${HDFS_PID_DIR}
export YARN_OPTS=''

export HADOOP_MAPRED_LOG_DIR=${YARN_LOG_DIR}

# YARN_OPTS="$YARN_OPTS -Dhadoop.log.dir=$YARN_LOG_DIR"
# YARN_OPTS="$YARN_OPTS -Dyarn.log.dir=$YARN_LOG_DIR"
# YARN_OPTS="$YARN_OPTS -Dhadoop.log.file=$YARN_LOGFILE"
# YARN_OPTS="$YARN_OPTS -Dyarn.log.file=$YARN_LOGFILE"
# YARN_OPTS="$YARN_OPTS -Dyarn.home.dir=$YARN_COMMON_HOME"
# YARN_OPTS="$YARN_OPTS -Dyarn.id.str=$YARN_IDENT_STRING"
# YARN_OPTS="$YARN_OPTS -Dhadoop.root.logger=${YARN_ROOT_LOGGER:-INFO,console}"
# YARN_OPTS="$YARN_OPTS -Dyarn.root.logger=${YARN_ROOT_LOGGER:-INFO,console}"


python /tmp/zk_wait.py resourcemanager

su - ${YARN_USER} -p -c "${HADOOP_HOME}/sbin/yarn-daemon.sh --config $HADOOP_CONF_DIR start resourcemanager"

# su - ${MAPRED_USER} -p -c "${HADOOP_HOME}/sbin/mr-jobhistory-daemon.sh --config ${HADOOP_CONF_DIR} start historyserver"

# we should actually check that the resourcemanager is up ...
python /tmp/zk_set.py resourcemanager up

echo log is ${YARN_LOG_DIR}/*resourcemanager-${HOSTNAME}.out

tail -f ${YARN_LOG_DIR}/*resourcemanager-${HOSTNAME}.out

