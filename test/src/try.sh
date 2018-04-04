#!/bin/bash
set -e

PYDOOP_CP=`python -c "import pydoop; print(pydoop.jar_path())"`
HADOOP_CP=`hadoop classpath`
CP="${PYDOOP_CP}:${HADOOP_CP}"

SRC="it/crs4/pydoop/mapreduce/pipes/TestPipesExternalSplits.java"
TESTED_CLASSES="it.crs4.pydoop.mapreduce.pipes.TestPipesExternalSplits"

javac -cp ${CP} ${SRC}

rm -f PYDOOP_INPUT PYDOOP_OUTPUT
hdfs dfs -rm -f PYDOOP_INPUT PYDOOP_OUTPUT

python build_pydoop_input.py 10 PYDOOP_INPUT
hdfs dfs -put PYDOOP_INPUT

for class in ${TESTED_CLASSES}
do 
    echo "Testing ${class}"
    java -cp $PWD:${CP} org.junit.runner.JUnitCore ${class}
done

hdfs dfs -get PYDOOP_OUTPUT

delta="`diff -q PYDOOP_INPUT PYDOOP_OUTPUT`"
if [ xx"${delta}" != "xx" ]; then
    exit 99
fi

