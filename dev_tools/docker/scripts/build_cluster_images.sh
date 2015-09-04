#!/bin/bash

TAG=${1}

CL_DIR=${TAG}/images

for d in ${CL_DIR}/*
do
    if [ -d ${d} -a -e ${d}/Dockerfile ]; then
        base=${d##${CL_DIR}/}
        docker build -t crs4_pydoop/${TAG}_${base} ${d}
    fi
done
         
exit

# docker build -t crs4_pydoop/${TAG}_base     ${CL_DIR}/base
# docker build -t crs4_pydoop/${TAG}_zookeeper ${CL_DIR}/zookeeper
# docker build -t crs4_pydoop/${TAG}_namenode ${CL_DIR}/namenode
# docker build -t crs4_pydoop/${TAG}_datanode ${CL_DIR}/datanode
# docker build -t crs4_pydoop/${TAG}_resourcemanager ${CL_DIR}/resourcemanager
# docker build -t crs4_pydoop/${TAG}_nodemanager ${CL_DIR}/nodemanager
# docker build -t crs4_pydoop/${TAG}_historyserver ${CL_DIR}/historyserver
# docker build -t crs4_pydoop/${TAG}_bootstrap     ${CL_DIR}/bootstrap


