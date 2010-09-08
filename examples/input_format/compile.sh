#!/bin/bash

# BEGIN_COPYRIGHT
# END_COPYRIGHT

HADOOP_HOME=${HADOOP_HOME:-/opt/hadoop}
SRC=net/sourceforge/pydoop/mapred/TextInputFormat.java

javac -classpath ${HADOOP_HOME}/hadoop-*-core.jar ${SRC}
if [ $? -ne 0 ]; then
    echo Compilation failed!
    exit 1
fi
jar -cvf pydoop-mapred.jar net/sourceforge/pydoop/mapred/TextInputFormat.class
