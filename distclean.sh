#!/bin/bash

rm -rf build
rm -rf dist
rm -fv \
    MANIFEST \
    src/SerialUtils.cpp \
    src/HadoopPipes.cpp \
    src/pydoop_pipes_main.cpp \
    src/pydoop_hdfs_main.cpp \
    src/StringUtils.cpp
find . -name '*.pyc' -exec rm -fv {} \;
