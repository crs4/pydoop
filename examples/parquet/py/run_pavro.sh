#!/bin/bash


PARQUET_JAR=../java/target/ParquetMR-assembly-0.1.jar
SCHEMA_FILE_LOCAL=../schemas/user.avsc
SCHEMA_FILE_HDFS=user.avsc

# ----- part 1 -----
INPUT_DATA=users.csv
PARQUETS_DIR=parquets
N=20
python ../java/create_input.py ${N} ${INPUT_DATA}

hdfs dfs -mkdir -p /user/${USER}
hdfs dfs -rmr /user/${USER}/${PARQUETS_DIR}
hdfs dfs -put -f ${INPUT_DATA}
hdfs dfs -put -f ${SCHEMA_FILE_LOCAL} ${SCHEMA_FILE_HDFS}
hadoop jar ${PARQUET_JAR} it.crs4.pydoop.ExampleParquetMRWrite \
    ${INPUT_DATA} ${PARQUETS_DIR} ${SCHEMA_FILE_HDFS}

# ----- part 3 -----
MODULE=pavropy
MPY=${MODULE}.py
JOBNAME=${MODULE}-job
LOGLEVEL=INFO
MRV="--mrv2"
USER_SCHEMA=`cat ${SCHEMA_FILE_LOCAL}`
INPUT_FORMAT=it.crs4.pydoop.PydoopAvroParquetInputFormat

INPUT=${PARQUETS_DIR}
OUTPUT=results

hdfs dfs -rmr /user/${USER}/${OUTPUT}

pydoop submit --upload-file-to-cache ${MPY} \
              --num-reducers 1 \
              --input-format ${INPUT_FORMAT} \
              -D parquet.avro.projection="${USER_SCHEMA}" \
              -D avro.schema="${USER_SCHEMA}" \
              --libjars ${PARQUET_JAR} \
              --log-level ${LOGLEVEL} ${MRV} --job-name ${JOBNAME} \
              ${MODULE} ${INPUT} ${OUTPUT}

# ----- part 4 -----
rm -rf ${OUTPUT}
hdfs dfs -get /user/${USER}/${OUTPUT}
# this is intentionally hardwired.
python check_results.py ${INPUT_DATA} ${OUTPUT}/part-r-00000
