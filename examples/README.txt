To run the examples, you need a working Hadoop cluster. If you don't
have one available right now, you can bring up a single-node Hadoop
cluster on your machine following instructions at:

http://hadoop.apache.org/common/docs/r0.20.1/quickstart.html

Configure Hadoop for "Pseudo-Distributed Operation" and start the
daemons. Now copy the "bin" directory with the python examples to
HDFS:

  ${HADOOP_HOME}/bin/hadoop dfs -put bin{,}

Upload an input directory for wordcount. You can use "bin" for this
too if you want:

  ${HADOOP_HOME}/bin/hadoop dfs -put bin input

To run the wordcount examples:

  ${HADOOP_HOME}/bin/hadoop pipes -conf conf/wordcount-minimal.xml \
    -input input -output output

  ${HADOOP_HOME}/bin/hadoop pipes -conf conf/wordcount-full.xml \
    -input input -output output
