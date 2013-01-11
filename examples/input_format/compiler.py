# BEGIN_COPYRIGHT
# 
# Copyright 2009-2013 CRS4.
# 
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy
# of the License at
# 
#   http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.
# 
# END_COPYRIGHT

import sys, os, subprocess, glob
import pydoop


SRC = "net/sourceforge/pydoop/mapred/TextInputFormat.java"
CLASS = SRC.replace("java", "class")


def main(argv):
  try:
    jar_name = argv[1]
  except IndexError:
    print "Usage: python %s JAR_NAME" % os.path.basename(argv[0])
    return 2
  if not os.path.isfile(jar_name):
    classpath = pydoop.hadoop_classpath()
    subprocess.check_call("javac -cp %s %s" % (classpath, SRC), shell=True) 
    subprocess.check_call("jar -cvf %s %s" % (jar_name, CLASS), shell=True)
  return 0


if __name__ == "__main__":
  sys.exit(main(sys.argv))
