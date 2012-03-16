# BEGIN_COPYRIGHT
# END_COPYRIGHT

import os, random

_DEFAULT_HDFS_HOST = "localhost"
_DEFAULT_HDFS_PORT = 9000
HDFS_HOST = os.getenv("HDFS_HOST", _DEFAULT_HDFS_HOST)
HDFS_PORT = os.getenv("HDFS_PORT", _DEFAULT_HDFS_PORT)
try:
  HDFS_PORT = int(HDFS_PORT)
except ValueError:
  raise ValueError("Environment variable HDFS_PORT must be an int")


def make_random_data(size):
  randint = random.randint
  return "".join([chr(randint(32, 126)) for _ in xrange(size)])
