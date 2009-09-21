#-------------------------------------
# utility functions

from pydoop_pipes import raise_pydoop_exception

DEFAULT_HDFS_PORT=9000

def jc_configure(obj, jc, k, f, df=None):
  v = df
  if jc.hasKey(k):
    v = jc.get(k)
  elif df is None:
    raise_pydoop_exception("jc_configure: no default for option '%s'" % k)
  setattr(obj, f, v)
#--
def jc_configure_int(obj, jc, k, f, df=None):
  v = df
  if jc.hasKey(k):
    v = jc.getInt(k)
  elif df is None:
    raise_pydoop_exception("jc_configure_int: no default for option '%s'" % k)
  setattr(obj, f, v)

#--
def jc_configure_bool(obj, jc, k, f, df=None):
  v = df
  if jc.hasKey(k):
    v = jc.getBoolean(k)
  elif df is None:
    raise_pydoop_exception("jc_configure_bool: no default for option '%s'" % k)
  setattr(obj, f, v)

#--
def jc_configure_float(obj, jc, k, f, df=None):
  v = df
  if jc.hasKey(k):
    v = jc.getFloat(k)
  elif df is None:
    raise_pydoop_exception("jc_configure_float: no default for option '%s'" % k)
  setattr(obj, f, v)

#--
import os
def split_hdfs_path(path):
  opath = path
  if not path.startswith('hdfs:'):
    raise_pydoop_exception('split_hdfs_path: illegal hdfs path <%s>' % opath)
  root, tail = '', ''
  while path.find(os.path.sep) > -1:
    root = os.path.join(tail, root)
    path, tail = os.path.split(path)
  if len(tail) == 0:
    raise_pydoop_exception('split_hdfs_path: illegal hdfs path <%s>' % opath)
  root = os.path.sep + root[:-1]
  parts = tail.split(':')
  if len(parts) == 1:
    return parts[0], DEFAULT_HDFS_PORT, root
  else:
    return parts[0], int(parts[1]), root
