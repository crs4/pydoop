#!/bin/bash

# FIXME: this example is not parquet-related

die() {
    echo $1 1>&2
    exit 1
}

nargs=1
if [ $# -ne $nargs ]; then
    die "Usage: $0 k|v|kv"
fi
mode=$1
if [ "${mode}" == "k" ]; then
    INPUT_FORMAT=it.crs4.pydoop.mapreduce.pipes.PydoopAvroKeyInputFormat
    MODULE=avro_key_in
elif [ "${mode}" == "v" ]; then
    INPUT_FORMAT=it.crs4.pydoop.mapreduce.pipes.PydoopAvroValueInputFormat
    MODULE=avro_value_in
elif [ "${mode}" == "kv" ]; then
    # FIXME: not supported yet
    INPUT_FORMAT=it.crs4.pydoop.mapreduce.pipes.PydoopAvroKeyValueInputFormat
    MODULE=avro_key_value_in
else
    die "invalid mode: ${mode}"
fi

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
MPY=${MODULE}.py
JOBNAME=${MODULE}-job
LOGLEVEL=DEBUG
MRV="--mrv2"
USER_SCHEMA=`cat ${SCHEMA_FILE_LOCAL}`

INPUT=${AVRO_FN}

hdfs dfs -rmr /user/${USER}/${OUTPUT}

# FIXME: auto-add installed avro-mapred jar
pydoop submit \
    --upload-file-to-cache avro_base.py \
    --upload-file-to-cache ${MPY} \
    --num-reducers 1 \
    --input-format ${INPUT_FORMAT} \
    --avro-input ${mode} \
    --libjars ${AVRO_MAPRED_JAR} \
    --log-level ${LOGLEVEL} \
    --job-name ${JOBNAME} \
     ${MRV} ${MODULE} ${INPUT} ${OUTPUT}

# --- check results ---
rm -rf ${OUTPUT}
hdfs dfs -get ${OUTPUT}
# this is intentionally hardwired.
python check_results.py ${CSV_FN} ${OUTPUT}/part-r-00000
