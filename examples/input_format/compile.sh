#!/bin/bash

# BEGIN_COPYRIGHT
# END_COPYRIGHT

HADOOP_HOME=${HADOOP_HOME:-/opt/hadoop}

bad_arg_exit() {
    echo $1
    exit 2
}

nargs=1
echo $#
if [ $# -ne $nargs ]; then
    bad_arg_exit "USAGE: $(basename $0) [mapred|mapreduce]"
fi

case $1 in
    mapred | mapreduce) MR=$1;;
    *) bad_arg_exit "bad argument: $1";;
esac

SRC=net/sourceforge/pydoop/${MR}/TextInputFormat.java

javac -classpath ${HADOOP_HOME}/hadoop-*-core.jar -d ./ ${SRC}
if [ $? -ne 0 ]; then
    echo Compilation failed!
    exit 1
fi
jar -cvf pydoop-${MR}.jar net/sourceforge/pydoop/${MR}/TextInputFormat.class
