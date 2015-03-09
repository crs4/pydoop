PARQUET_JAR=../java/target/ParquetMR-assembly-0.1.jar
MODULE=kmer_count
MPY=${MODULE}.py
JOBNAME=${MODULE}-job
LOGLEVEL=DEBUG
MRV="--mrv2"
INPUT_FORMAT=parquet.avro.AvroParquetInputFormat
PROJECTION=`cat ../schemas/alignment_record_proj.avsc`

INPUT=mini_aligned_seqs.gz.parquet
OUTPUT=kmer_count

hdfs dfs -mkdir -p /user/${USER}
hdfs dfs -rmr /user/${USER}/${INPUT}
hdfs dfs -put -f ${INPUT}
hdfs dfs -rmr /user/${USER}/${OUTPUT}

pydoop submit \
     -D parquet.avro.projection="${PROJECTION}" \
    --upload-file-to-cache ${MPY} \
    --num-reducers 1 \
    --input-format ${INPUT_FORMAT} \
    --avro-input v \
    --libjars ${PARQUET_JAR} \
    --log-level ${LOGLEVEL} ${MRV} \
    --job-name ${JOBNAME} \
    ${MODULE} ${INPUT} ${OUTPUT}


rm -rf ${OUTPUT}
hdfs dfs -get /user/${USER}/${OUTPUT}
python show_kmer_count.py ${OUTPUT}/part-r-00000
