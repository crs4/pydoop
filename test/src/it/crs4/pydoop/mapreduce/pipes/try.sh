#!/bin/bash

PYDOOP_CP=`pydoop classpath`
HADOOP_CP=`hadoop classpath`
CP="${PYDOOP_CP}:${HADOOP_CP}"

SRC="TestPipesNonJavaInputFormat.java"
TESTED_CLASSES="TestPipesExternalSplits"

rm -rf test_classes
javac -cp ${CP} ${SRC} -d test_classes


for class in ${TESTED_CLASSES}
do 
    echo "Testing ${class}"
    java -cp $PWD/test_classes:${CP} \
         org.junit.runner.JUnitCore it.crs4.pydoop.pipes.mapreduce.${class}
done
