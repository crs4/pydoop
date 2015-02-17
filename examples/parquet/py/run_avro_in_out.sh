#!/bin/bash


PARQUET_JAR=../java/target/ParquetMR-assembly-0.1.jar
IN_SCHEMA_FILE_LOCAL=../../avro/schemas/user.avsc
IN_SCHEMA_FILE_HDFS=user.avsc
OUT_SCHEMA_FILE_LOCAL=../../avro/schemas/stats.avsc
OUT_SCHEMA_FILE_HDFS=stats.avsc


# --- create input ---
INPUT_DATA=users.csv
PARQUETS_DIR=parquets
N=20
python ../java/create_input.py ${N} ${INPUT_DATA}

hdfs dfs -mkdir -p /user/${USER}
hdfs dfs -rmr /user/${USER}/${PARQUETS_DIR}
hdfs dfs -put -f ${INPUT_DATA}
hdfs dfs -put -f ${IN_SCHEMA_FILE_LOCAL} ${IN_SCHEMA_FILE_HDFS}
hadoop jar ${PARQUET_JAR} it.crs4.pydoop.ExampleParquetMRWrite \
    ${INPUT_DATA} ${PARQUETS_DIR} ${IN_SCHEMA_FILE_HDFS}


# --- run color count ---
MODULE=avro_in_out
MPY=${MODULE}.py
JOBNAME=${MODULE}-job
LOGLEVEL=DEBUG
MRV="--mrv2"
STATS_SCHEMA=`cat ${OUT_SCHEMA_FILE_LOCAL}`
INPUT_FORMAT=parquet.avro.AvroParquetInputFormat
OUTPUT_FORMAT=parquet.avro.AvroParquetOutputFormat

INPUT=${PARQUETS_DIR}
OUTPUT=results

hdfs dfs -rmr /user/${USER}/${OUTPUT}

pydoop submit \
    -D pydoop.mapreduce.avro.value.output.schema="${STATS_SCHEMA}" \
    -D parquet.avro.schema="${STATS_SCHEMA}" \
    --upload-file-to-cache ${MPY} \
    --num-reducers 1 \
    --input-format ${INPUT_FORMAT} \
    --output-format ${OUTPUT_FORMAT} \
    --avro-input v \
    --avro-output v \
    --libjars ${PARQUET_JAR} \
    --log-level ${LOGLEVEL} ${MRV} \
    --job-name ${JOBNAME} \
    ${MODULE} ${INPUT} ${OUTPUT}


# --- check results ---
# FIXME: TBD
