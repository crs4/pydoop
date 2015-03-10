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
    MODULE=avro_key_in_out
elif [ "${mode}" == "v" ]; then
    MODULE=avro_value_in_out
elif [ "${mode}" == "kv" ]; then
    MODULE=avro_key_value_in_out
else
    die "invalid mode: ${mode}"
fi

USER_SCHEMA_FILE=../schemas/user.avsc
PET_SCHEMA_FILE=../schemas/pet.avsc
STATS_SCHEMA_FILE=../schemas/stats.avsc
STATS_SCHEMA=`cat ${STATS_SCHEMA_FILE}`
CSV_FN=users.csv
AVRO_FN=users.avro  # used also for KV
OUTPUT=results

# --- generate avro input ---
N=20
python create_input.py ${N} ${CSV_FN}
if [ "${mode}" == "kv" ]; then
    pushd ../java >/dev/null
    ./write_avro_kv ${USER_SCHEMA_FILE} ${PET_SCHEMA_FILE} \
	../py/${CSV_FN} ../py/${AVRO_FN}
    popd >/dev/null
else
    python write_avro.py ${USER_SCHEMA_FILE} ${CSV_FN} ${AVRO_FN}
fi
hdfs dfs -mkdir -p /user/${USER}
hdfs dfs -rm ${AVRO_FN}
hdfs dfs -put ${AVRO_FN}

# --- run cc ---
MPY=${MODULE}.py
JOBNAME=${MODULE}-job
LOGLEVEL=DEBUG
MRV="--mrv2"

INPUT=${AVRO_FN}

# put the following opts at the end of the command line
# or the empty string will be parsed as the module arg
if [ "${mode}" == "k" ]; then
    K_SCHEMA_OPT="-D pydoop.mapreduce.avro.key.output.schema=${STATS_SCHEMA}"
    V_SCHEMA_OPT=""
elif [ "${mode}" == "v" ]; then
    K_SCHEMA_OPT=""
    V_SCHEMA_OPT="-D pydoop.mapreduce.avro.value.output.schema=${STATS_SCHEMA}"
else
    K_SCHEMA_OPT="-D pydoop.mapreduce.avro.key.output.schema=${STATS_SCHEMA}"
    V_SCHEMA_OPT="-D pydoop.mapreduce.avro.value.output.schema=${STATS_SCHEMA}"
fi

hdfs dfs -rmr /user/${USER}/${OUTPUT}

pydoop submit \
    --upload-file-to-cache avro_base.py \
    --upload-file-to-cache ${MPY} \
    --num-reducers 1 \
    --avro-input ${mode} \
    --avro-output ${mode} \
    --log-level ${LOGLEVEL} \
    --job-name ${JOBNAME} \
     ${MRV} ${MODULE} ${INPUT} ${OUTPUT} \
    "${K_SCHEMA_OPT}" "${V_SCHEMA_OPT}"


# --- dump & check results ---
DUMP_FN=stats.tsv
rm -rf ${OUTPUT}
hdfs dfs -get ${OUTPUT}
python avro_container_dump_results.py \
    ${OUTPUT}/part-r-00000.avro ${DUMP_FN} ${mode}
python check_results.py ${CSV_FN} ${DUMP_FN}
