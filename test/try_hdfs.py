# BEGIN_COPYRIGHT
# 
# Copyright 2012 CRS4.
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

"""
Check that reloading the hdfs module after changing
os.environ['HADOOP_CONF_DIR'] works (i.e., Pydoop references the
correct HDFS service).

Note that it does **NOT** work if you've already instantiated an hdfs
handle, and this is NOT due to the caching system.
"""

import sys, os

import pydoop.hdfs as hdfs
import pydoop.hadut as hadut


def dump_status(fs):
  print "(host, port, user) = %r" % ((fs.host, fs.port, fs.user),)
  print "_CACHE = %r" % (fs._CACHE,)
  print "_ALIASES = %r" % (fs._ALIASES,)
  print


def main(argv=sys.argv):
  try:
    hcd = argv[1]
  except IndexError:
    sys.exit("Usage: python %s HADOOP_CONF_DIR" % os.path.basename(argv[0]))
  os.environ["HADOOP_CONF_DIR"] = os.path.abspath(hcd)
  reload(hdfs)  # or: hdfs.init()
  fs1 = hdfs.hdfs()
  print "--- OPEN ---"
  dump_status(fs1)
  for fs in [fs1]:
    fs.close()
  print "--- CLOSED ---"
  dump_status(fs1)
  print "task trackers (truncated):", hadut.get_task_trackers()[:3]


if __name__ == "__main__":
  main()
