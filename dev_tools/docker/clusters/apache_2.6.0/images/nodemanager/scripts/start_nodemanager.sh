#!/bin/bash

export YARN_LOG_DIR=${YARN_LOG_DIR}
export HADOOP_PID_DIR=${HDFS_PID_DIR}

python /tmp/zk_wait.py nodemanager

# YARN_OPTS="$YARN_OPTS -Dhadoop.log.dir=$YARN_LOG_DIR"
# YARN_OPTS="$YARN_OPTS -Dyarn.log.dir=$YARN_LOG_DIR"
# YARN_OPTS="$YARN_OPTS -Dhadoop.log.file=$YARN_LOGFILE"
# YARN_OPTS="$YARN_OPTS -Dyarn.log.file=$YARN_LOGFILE"
# YARN_OPTS="$YARN_OPTS -Dyarn.home.dir=$YARN_COMMON_HOME"
# YARN_OPTS="$YARN_OPTS -Dyarn.id.str=$YARN_IDENT_STRING"
# YARN_OPTS="$YARN_OPTS -Dhadoop.root.logger=${YARN_ROOT_LOGGER:-INFO,console}"
# YARN_OPTS="$YARN_OPTS -Dyarn.root.logger=${YARN_ROOT_LOGGER:-INFO,console}"


su - ${YARN_USER} -p -c "${HADOOP_HOME}/sbin/yarn-daemon.sh --config $HADOOP_CONF_DIR start nodemanager"

# we should actually check that the nodemanager is up ...
python /tmp/zk_set.py nodemanager up

echo log is ${YARN_LOG_DIR}/*nodemanager-${HOSTNAME}.out

tail -f ${YARN_LOG_DIR}/*nodemanager-${HOSTNAME}.out

