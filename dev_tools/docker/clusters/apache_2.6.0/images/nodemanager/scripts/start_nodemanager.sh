#!/bin/bash

python /tmp/zk_wait.py nodemanager

# we should actually check that the nodemanager is up ...
python /tmp/zk_set.py nodemanager up

su ${YARN_USER} -c "${HADOOP_HOME}/bin/yarn --config ${HADOOP_CONF_DIR} nodemanager 2>&1 >/tmp/logs/nodemanager.out"
