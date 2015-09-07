#!/bin/bash

die() {
    echo $1 1>&2
    exit 1
}

nargs=2
if [ $# -ne $nargs ]; then
    die "Usage: $0 N_RECORDS ARRAY_SIZE"
fi
nrec=$1
size=$2

# --- generate input ---
fin="random_arrays.in"
( for i in $(seq 1 ${nrec}); do echo -ne "R${i}\t${size}\n"; done ) > ${fin}
hdfs dfs -mkdir -p /user/${USER}
hdfs dfs -rm ${fin}
hdfs dfs -put ${fin}

# --- run ---
MODULE="gen_data"
MPY=${MODULE}.py
JOBNAME=${MODULE}-job
LOGLEVEL=DEBUG
MRV=""
INPUT=${fin}
OUTPUT=$(echo ${fin} | cut -d '.' -f 1).avro
SCHEMA=$(cat ../schemas/named_arr.avsc)

hdfs dfs -rmr /user/${USER}/${OUTPUT}
pydoop submit \
    -D pydoop.mapreduce.avro.value.output.schema="${SCHEMA}" \
    -D mapreduce.input.lineinputformat.linespermap=100 \
    --input-format org.apache.hadoop.mapreduce.lib.input.NLineInputFormat \
    --upload-file-to-cache ${MPY} \
    --num-reducers 0 \
    --avro-output v \
    --log-level ${LOGLEVEL} \
    --job-name ${JOBNAME} \
     ${MRV} ${MODULE} ${INPUT} ${OUTPUT}
