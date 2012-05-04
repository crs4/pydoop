# BEGIN_COPYRIGHT
# END_COPYRIGHT

import sys, os, subprocess, glob
import pydoop

HADOOP_HOME = pydoop.hadoop_home()
HADOOP_VERSION = pydoop.hadoop_version()
SRC = "net/sourceforge/pydoop/mapred/TextInputFormat.java"
CLASS = SRC.replace("java", "class")


def main(argv):
  try:
    jar_name = argv[1]
  except IndexError:
    print "Usage: python %s JAR_NAME" % os.path.basename(argv[0])
    return 2
  if not os.path.isfile(jar_name):
    hadoop_jars = glob.glob(os.path.join(HADOOP_HOME, "hadoop-*.jar"))
    classpath = ":".join(hadoop_jars)
    subprocess.check_call("javac -cp %s %s" % (classpath, SRC), shell=True) 
    subprocess.check_call("jar -cvf %s %s" % (jar_name, CLASS), shell=True)
  return 0


if __name__ == "__main__":
  sys.exit(main(sys.argv))
