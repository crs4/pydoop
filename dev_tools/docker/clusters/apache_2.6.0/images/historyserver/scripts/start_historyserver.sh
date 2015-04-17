#!/bin/bash

python /tmp/zk_wait.py historyserver

# we should actually check that the nodemanager is up ...
python /tmp/zk_set.py historyserver up

su ${MAPRED_USER} -c "${HADOOP_HOME}/bin/mapred --config ${HADOOP_CONF_DIR} historyserver 2>&1 >/tmp/logs/historyserver.out"


