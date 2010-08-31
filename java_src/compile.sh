#!/bin/bash

HADOOP_HOME=${HADOOP_HOME:-/opt/hadoop}

nargs=1
echo $#
if [ $# -ne $nargs ]; then
    echo "USAGE: $(basename $0) SRC_FILE"
    exit 2
fi
SRC=$1

javac -classpath ${HADOOP_HOME}/hadoop-*-core.jar -d ./ ${SRC}
if [ $? -ne 0 ]; then
    echo Compilation failed!
    exit 1
fi
jar -cvf pydoop.jar -C ./ .
