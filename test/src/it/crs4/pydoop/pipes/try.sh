#!/bin/bash
export HADOOP_PREFIX=/opt/hadoop
export PATH=${HADOOP_PREFIX}/bin:${PATH}

PYDRIVER=/home/zag/work/vs/git/pydoop/src/it/crs4/pydoop/pypipes_classes


SRC="TestPipeApplication.java TestPipesNonJavaInputFormat.java CommonStub.java PipeApplicationStub.java PipeApplicationRunnableStub.java DummyInputFormat.java PipeReducerStub.java"
javac -cp `hadoop classpath`:${PYDRIVER} ${SRC} -d test_classes

for class in TestPipeApplication TestPipesNonJavaInputFormat
do 
    echo "Testing ${class}"
    java -cp $PWD/test_classes:`hadoop classpath`:${PYDRIVER} org.junit.runner.JUnitCore it.crs4.pydoop.pipes.${class}
done

exit
rm pypipes.jar; jar -cvf pypipes.jar -C pypipes_classes/ .

hadoop jar pypipes.jar it.crs4.pydoop.pipes.Submitter -libjars pypipes.jar \
           -D hadoop.pipes.java.recordreader=true -D hadoop.pipes.java.recordwriter=true \
           -inputformat it.crs4.pydoop.input.PydoopInputFormat \
           -program user_counts.py -reduces 0 -input input -output output

