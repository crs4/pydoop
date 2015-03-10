#!/bin/bash


USER_SCHEMA_FILE=../schemas/user.avsc
STATS_SCHEMA_FILE=../schemas/stats.avsc
CSV_FN=users.csv
AVRO_FN=users.avro
OUTPUT=results


# --- generate avro input ---
N=20
python create_input.py ${N} ${CSV_FN}
python write_avro.py ${USER_SCHEMA_FILE} ${CSV_FN} ${AVRO_FN}
hdfs dfs -mkdir -p /user/${USER}
hdfs dfs -rm ${AVRO_FN}
hdfs dfs -put ${AVRO_FN}


# --- run cc ---
MODULE=avro_pyrw
MPY=${MODULE}.py
JOBNAME=${MODULE}-job
LOGLEVEL=DEBUG
MRV="--mrv2"
INPUT=${AVRO_FN}

hdfs dfs -rmr /user/${USER}/${OUTPUT}
pydoop submit \
    --upload-file-to-cache ${MPY} \
    --upload-file-to-cache ${USER_SCHEMA_FILE} \
    --upload-file-to-cache ${STATS_SCHEMA_FILE} \
    --num-reducers 1 \
    -D mapreduce.pipes.isjavarecordreader=false \
    -D mapreduce.pipes.isjavarecordwriter=false \
    --log-level ${LOGLEVEL} \
    --job-name ${JOBNAME} \
    ${MRV} ${MODULE} ${INPUT} ${OUTPUT}


# --- dump & check results ---
DUMP_FN=stats.tsv
rm -rf ${OUTPUT}
hdfs dfs -get ${OUTPUT}
python avro_container_dump_results.py \
    ${OUTPUT}/part-r-00000.avro ${DUMP_FN}
python check_results.py ${CSV_FN} ${DUMP_FN}
