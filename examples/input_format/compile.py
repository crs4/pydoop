#!/usr/bin/env python

# BEGIN_COPYRIGHT
# END_COPYRIGHT

import sys, os, subprocess
from pydoop.hadoop_utils import get_hadoop_version

HADOOP_HOME = os.getenv("HADOOP_HOME", "/opt/hadoop")
HADOOP_VERSION = get_hadoop_version(HADOOP_HOME)
SRC = "net/sourceforge/pydoop/mapred/TextInputFormat.java"
CLASS = SRC.replace("java", "class")
OUTPUT_JAR = "pydoop-mapred.jar"


def string_version(tuple_version):
  return ".".join(map(str, HADOOP_VERSION))


def main(argv):
  if HADOOP_VERSION < (0,21,0):
    hadoop_jars = ["hadoop-%s-core.jar" % string_version(HADOOP_VERSION)]
  else:
    hadoop_jars = ["hadoop-%s-%s.jar" % (tag, string_version(HADOOP_VERSION))
                   for tag in ("common", "hdfs", "mapred")]
  classpath = ":".join([os.path.join(HADOOP_HOME, jar) for jar in hadoop_jars])
  subprocess.check_call("javac -cp %s %s" % (classpath, SRC), shell=True) 
  subprocess.check_call("jar -cvf %s %s" % (OUTPUT_JAR, CLASS), shell=True)


if __name__ == "__main__":
  main(sys.argv)
