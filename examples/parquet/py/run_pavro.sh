#!/bin/bash


PARQUET_JAR=../java/target/ParquetMR-assembly-0.1.jar
AVRO_USER_AVSC=../../avro/schemas/user.avsc

SUBMIT_CMD="/home/zag/.local/bin/pydoop submit"


# ----- part 1 -----
INPUT_DATA=users.csv
PARQUETS_DIR=parquets
N=20
python ../java/create_input.py ${N} ${INPUT_DATA}

hdfs dfs -mkdir -p /user/${USER}
hdfs dfs -rmr /user/${USER}/${PARQUETS_DIR}
hdfs dfs -put -f ${INPUT_DATA}
hadoop jar ${PARQUET_JAR} it.crs4.pydoop.ExampleParquetMRWrite \
                          ${INPUT_DATA} ${PARQUETS_DIR}

# ----- part 3 -----
MODULE=pavropy
MZIP=${MODULE}.zip
MPY=${MODULE}.py
PROGNAME=${MODULE}-prog
JOBNAME=${MODULE}-job
LOGLEVEL=INFO
MRV="--mrv2"
USER_SCHEMA=`cat ${AVRO_USER_AVSC}`

INPUT=${PARQUETS_DIR}
OUTPUT=results

hdfs dfs -rmr /user/${USER}/${OUTPUT}

zip ${MZIP} ${MPY}
${SUBMIT_CMD} --python-egg ${MZIP} \
              -D mapreduce.pipes.isjavarecordreader=true \
              -D mapreduce.pipes.isjavarecordwriter=true \
              --num-reducers 1 \
              --input-format it.crs4.pydoop.PydoopAvroParquetInputFormat \
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
