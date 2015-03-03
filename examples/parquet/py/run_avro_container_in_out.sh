#!/bin/bash

# FIXME: this example is not parquet-related

die() {
    echo $1 1>&2
    exit 1
}

USER_SCHEMA_FN=../../avro/schemas/user.avsc
STATS_SCHEMA_FN=../../avro/schemas/stats.avsc
STATS_SCHEMA=`cat ${STATS_SCHEMA_FN}`
CSV_FN=users.csv
AVRO_FN=users.avro
OUTPUT=results

nargs=1
if [ $# -ne $nargs ]; then
    die "Usage: $0 k|v|kv"
fi
mode=$1
if [ "${mode}" == "k" ]; then
    INPUT_FORMAT=it.crs4.pydoop.mapreduce.pipes.PydoopAvroKeyInputFormat
    OUTPUT_FORMAT=it.crs4.pydoop.mapreduce.pipes.PydoopAvroKeyOutputFormat
    OUT_SCHEMA_PROP=pydoop.mapreduce.avro.key.output.schema
    MODULE=avro_key_in_out
elif [ "${mode}" == "v" ]; then
    INPUT_FORMAT=it.crs4.pydoop.mapreduce.pipes.PydoopAvroValueInputFormat
    OUTPUT_FORMAT=it.crs4.pydoop.mapreduce.pipes.PydoopAvroValueOutputFormat
    OUT_SCHEMA_PROP=pydoop.mapreduce.avro.value.output.schema
    MODULE=avro_value_in_out
elif [ "${mode}" == "kv" ]; then
    # FIXME: not supported yet
    INPUT_FORMAT=it.crs4.pydoop.mapreduce.pipes.PydoopAvroKeyValueInputFormat
    OUTPUT_FORMAT=it.crs4.pydoop.mapreduce.pipes.PydoopAvroKeyValueOutputFormat
    MODULE=avro_key_value_in_out
else
    die "invalid mode: ${mode}"
fi


# --- generate avro input ---
N=20
python ../java/create_input.py ${N} ${CSV_FN}
python write_avro.py ${USER_SCHEMA_FN} ${CSV_FN} ${AVRO_FN}

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
    -D "${OUT_SCHEMA_PROP}"="${STATS_SCHEMA}" \
    --upload-file-to-cache avro_base.py \
    --upload-file-to-cache ${MPY} \
    --num-reducers 1 \
    --input-format ${INPUT_FORMAT} \
    --output-format ${OUTPUT_FORMAT} \
    --avro-input ${mode} \
    --avro-output ${mode} \
    --log-level ${LOGLEVEL} \
    --job-name ${JOBNAME} \
     ${MRV} ${MODULE} ${INPUT} ${OUTPUT}


# --- dump & check results ---
DUMP_FN=stats.tsv
rm -rf ${OUTPUT}
hdfs dfs -get ${OUTPUT}
python avro_container_dump_results.py ${OUTPUT}/part-r-00000.avro ${DUMP_FN}
python check_results.py ${CSV_FN} ${DUMP_FN}
