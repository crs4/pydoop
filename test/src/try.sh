#!/bin/bash

PYDOOP_CP=`python -c "import pydoop; print(pydoop.jar_path())"`
HADOOP_CP=`hadoop classpath`
CP="${PYDOOP_CP}:${HADOOP_CP}"

SRC="it/crs4/pydoop/mapreduce/pipes/TestPipesExternalSplits.java"
TESTED_CLASSES="it.crs4.pydoop.mapreduce.pipes.TestPipesExternalSplits"

javac -cp ${CP} ${SRC}


for class in ${TESTED_CLASSES}
do 
    echo "Testing ${class}"
    java -cp $PWD:${CP} org.junit.runner.JUnitCore ${class}
done
