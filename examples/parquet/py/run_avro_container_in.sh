#!/bin/bash

# FIXME: this example is not parquet-related

SCHEMA_FILE_LOCAL=../../avro/schemas/user.avsc
AVRO_MAPRED_JAR=../../../lib/avro-mapred-1.7.4-hadoop2.jar
CSV_FN=users.csv
AVRO_FN=users.avro
OUTPUT=results

# --- generate avro input ---
N=20
python ../java/create_input.py ${N} ${CSV_FN}
python write_avro.py ${SCHEMA_FILE_LOCAL} ${CSV_FN} ${AVRO_FN}

hdfs dfs -mkdir -p /user/${USER}
hdfs dfs -rm ${AVRO_FN}
hdfs dfs -put ${AVRO_FN}

# --- run cc ---
MODULE=avro_in
MPY=${MODULE}.py
JOBNAME=${MODULE}-job
LOGLEVEL=DEBUG
MRV="--mrv2"
USER_SCHEMA=`cat ${SCHEMA_FILE_LOCAL}`
INPUT_FORMAT=it.crs4.pydoop.mapreduce.pipes.PydoopAvroValueInputFormat

INPUT=${AVRO_FN}

hdfs dfs -rmr /user/${USER}/${OUTPUT}

# FIXME: auto-add installed avro-mapred jar
pydoop submit --upload-file-to-cache ${MPY} \
              --num-reducers 1 \
              --input-format ${INPUT_FORMAT} \
              --avro-input v \
              --libjars ${AVRO_MAPRED_JAR} \
              --log-level ${LOGLEVEL} ${MRV} --job-name ${JOBNAME} \
              ${MODULE} ${INPUT} ${OUTPUT}

# --- check results ---
rm -rf ${OUTPUT}
hdfs dfs -get ${OUTPUT}
# this is intentionally hardwired.
python check_results.py ${CSV_FN} ${OUTPUT}/part-r-00000
