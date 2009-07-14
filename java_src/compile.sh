HADOOP_HOME=${HADOOP_HOME:-/opt/hadoop}
javac -classpath ${HADOOP_HOME}/hadoop-*-core.jar \
      -d pydoop_classes TextInputFormat.java
jar -cvf pydoop.jar -C pydoop_classes/ .
