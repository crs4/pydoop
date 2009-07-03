
from pydoop_hdfs import hdfs_fs as hdfs

#-------------------------------------
# utility functions

DEFAULT_HDFS_PORT=9000

import os
def split_hdfs_path(path):
  root = ''
  tail = ''
  while path.find(os.path.sep) > -1:
    root = os.path.join(tail, root)
    path, tail = os.path.split(path)
  assert path == 'hdfs:' and len(tail) > 0
  root = os.path.sep + root[:-1]
  parts = tail.split(':')
  if len(parts) == 1:
    return parts[0], DEFAULT_HDFS_PORT, root
  else:
    return parts[0], int(parts[1]), root
