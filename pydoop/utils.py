#-------------------------------------
# utility functions

DEFAULT_HDFS_PORT=9000

def jc_configure(obj, jc, k, f, df=None):
  v = df
  if jc.hasKey(k):
    v = jc.get(k)
  elif not df:
    raise ValueError("No default for option '%s'" % k)
  setattr(obj, f, v)
#--
def jc_configure_int(obj, jc, k, f, df=None):
  v = df
  if jc.hasKey(k):
    v = jc.getInt(k)
  elif not df:
    raise ValueError("No default for option '%s'" % k)    
  setattr(obj, f, v)

#--
def jc_configure_bool(obj, jc, k, f, df):
  v = df
  if jc.hasKey(k):
    v = jc.getBoolean(k)
  setattr(obj, f, v)

#--
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
