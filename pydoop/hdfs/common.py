import getpass

BUFSIZE = 16384
DEFAULT_PORT = 8020  # org/apache/hadoop/hdfs/server/namenode/NameNode.java
DEFAULT_USER = getpass.getuser()
DEFAULT_LIBHDFS_OPTS = "-Xmx48m"  # enough for most applications
