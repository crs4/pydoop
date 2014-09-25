#!/bin/bash
export HADOOP_PREFIX=/opt/hadoop
export PATH=${HADOOP_PREFIX}/bin:${PATH}

javac -cp `hadoop classpath` pipes/*.java -d pypipes_classes

exit
rm pypipes.jar; jar -cvf pypipes.jar -C pypipes_classes/ .

hadoop jar pypipes.jar it.crs4.pydoop.pipes.Submitter -libjars pypipes.jar \
           -D hadoop.pipes.java.recordreader=true -D hadoop.pipes.java.recordwriter=true \
           -inputformat it.crs4.pydoop.input.PydoopInputFormat \
           -program user_counts.py -reduces 0 -input input -output output

