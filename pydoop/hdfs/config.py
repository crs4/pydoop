# BEGIN_COPYRIGHT
# END_COPYRIGHT

"""
HDFS configuration.
"""

import os, glob
import pydoop


BUFSIZE = 16384
DEFAULT_PORT = 8020  # org/apache/hadoop/hdfs/server/namenode/NameNode.java
DEFAULT_USER = os.environ["USER"]

try:
  _ORIG_CLASSPATH
except NameError:
  _ORIG_CLASSPATH = os.getenv("CLASSPATH", "")
_HADOOP_HOME = pydoop.hadoop_home()
_HADOOP_CONF_DIR = pydoop.hadoop_conf()
_JARS = (
  glob.glob(os.path.join(_HADOOP_HOME, "lib/*.jar")) +
  glob.glob(os.path.join(_HADOOP_HOME, "hadoop*.jar")) +
  [_HADOOP_CONF_DIR]
  )
_CLASSPATH = "%s:%s" % (":".join(_JARS), _ORIG_CLASSPATH)
os.environ["CLASSPATH"] = _CLASSPATH

_DEFAULT_LIBHDFS_OPTS = "-Xmx48m"  # enough for most applications
os.environ["LIBHDFS_OPTS"] = os.getenv("LIBHDFS_OPTS", _DEFAULT_LIBHDFS_OPTS)
