# TODO: rewrite

# bin/hadoop pipes \
#   [-input inputDir] \
#   [-output outputDir] \
#   [-jar applicationJarFile] \
#   [-inputformat class] \
#   [-map class] \
#   [-partitioner class] \
#   [-reduce class] \
#   [-writer class] \
#   [-program program url] \ 
#   [-conf configuration file] \
#   [-D property=value] \
#   [-fs local|namenode:port] \
#   [-jt local|jobtracker:port] \
#   [-files comma separated list of files] \ 
#   [-libjars comma separated list of jars] \
#   [-archives comma separated list of archives] 

# -D keep.failed.task.files=true,hadoop.pipes.command-file.keep=true

PROGRAM=/examples/bin/WordCount
INDIR=/examples/in-dir
OUTDIR=/examples/out-dir
HADOOP_HOME=/opt/hadoop-0.19.1
CONF_FILE=conf/word.xml
${HADOOP_HOME}/bin/hadoop pipes -conf ${CONF_FILE} -input ${INDIR} -output ${OUTDIR} -program ${PROGRAM}


