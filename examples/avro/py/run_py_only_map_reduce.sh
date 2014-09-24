#!/bin/bash

MODULE=color_counts
MZIP=${MODULE}.zip
MPY=${MODULE}.py
AVRO_USER_AVSC=../user.avsc
AVRO_STATS_AVSC=../stats.avsc
AVRO_DATA=users.avro

PROGNAME=${MODULE}-prog
JOBNAME=${MODULE}-job

LOGLEVEL=DEBUG
MRV="--mrv2"

INPUT=input
OUTPUT=output

SUBMIT_CMD="/home/zag/.local/bin/pydoop submit"

zip ${MZIP} ${MPY}
hdfs dfs -mkdir -p /user/${USER}/${INPUT}

hdfs dfs -rmr /user/${USER}/${OUTPUT}
hdfs dfs -put -f ${AVRO_DATA} ${INPUT}

${SUBMIT_CMD} --python-egg ${MZIP} --upload-to-cache ${AVRO_STATS_AVSC} \
                                   --upload-to-cache ${AVRO_USER_AVSC} \
              -D mapreduce.pipes.isjavarecordreader=false \
              -D mapreduce.pipes.isjavarecordwriter=false \
              --module ${MODULE} \
              --log-level ${LOGLEVEL} ${MRV} --job-name ${JOBNAME} \
              ${PROGNAME} ${INPUT} ${OUTPUT} 

