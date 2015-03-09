#!/bin/bash

MODULE="color_count"
USER_SCHEMA_FILE=../schemas/user.avsc
STATS_SCHEMA_FILE=../schemas/stats.avsc
STATS_SCHEMA=`cat ${STATS_SCHEMA_FILE}`
CSV_FN=users.csv
AVRO_FN=users.avro  # used also for KV
OUTPUT=results

# --- generate avro input ---
N=20
python generate_avro_users.py ${USER_SCHEMA_FILE} ${N} ${AVRO_FN}
hdfs dfs -mkdir -p /user/${USER}
hdfs dfs -rm ${AVRO_FN}
hdfs dfs -put ${AVRO_FN}

# --- run cc ---
MPY=${MODULE}.py
JOBNAME=${MODULE}-job
LOGLEVEL=DEBUG
MRV="--mrv2"
INPUT=${AVRO_FN}

hdfs dfs -rmr /user/${USER}/${OUTPUT}
pydoop submit \
    -D pydoop.mapreduce.avro.value.output.schema="${STATS_SCHEMA}" \
    --upload-file-to-cache ${MPY} \
    --num-reducers 1 \
    --avro-input v \
    --avro-output v \
    --log-level ${LOGLEVEL} \
    --job-name ${JOBNAME} \
     ${MRV} ${MODULE} ${INPUT} ${OUTPUT}


# --- dump & check results ---
DUMP_FN=stats.tsv
rm -rf ${OUTPUT}
hdfs dfs -get ${OUTPUT}
python avro_container_dump_results.py ${OUTPUT}/part-r-00000.avro ${DUMP_FN}
