# BEGIN_COPYRIGHT
# END_COPYRIGHT
"""
Contains miscellaneous utility functions.
"""

import os
from urlparse import urlparse
from struct import pack
from pydoop_pipes import raise_pydoop_exception
from pydoop_pipes import quote_string, unquote_string 
from pipes_runner import pipes_runner


DEFAULT_HDFS_PORT=9000


def jc_configure(obj, jc, k, f, df=None):
  """
  Gets a configuration parameter from C{jc} and automatically sets a
  corresponding attribute on C{obj}.

  @param obj: object on which the attribute must be set
  @param jc: a C{JobConf} object
  @param k: a configuration key
  @param f: name of the attribute to set
  @param df: default value for the attribute if C{k} is not present in C{jc}
  """
  v = df
  if jc.hasKey(k):
    v = jc.get(k)
  elif df is None:
    raise_pydoop_exception("jc_configure: no default for option '%s'" % k)
  setattr(obj, f, v)


def jc_configure_int(obj, jc, k, f, df=None):
  """
  Works like L{jc_configure}, but converts C{jc[k]} to an int.
  """
  v = df
  if jc.hasKey(k):
    v = jc.getInt(k)
  elif df is None:
    raise_pydoop_exception("jc_configure_int: no default for option '%s'" % k)
  setattr(obj, f, v)


def jc_configure_bool(obj, jc, k, f, df=None):
  """
  Works like L{jc_configure}, but converts C{jc[k]} to a bool.
  """
  v = df
  if jc.hasKey(k):
    v = jc.getBoolean(k)
  elif df is None:
    raise_pydoop_exception("jc_configure_bool: no default for option '%s'" % k)
  setattr(obj, f, v)


def jc_configure_float(obj, jc, k, f, df=None):
  """
  Works like L{jc_configure}, but converts C{jc[k]} to a float.
  """
  v = df
  if jc.hasKey(k):
    v = jc.getFloat(k)
  elif df is None:
    raise_pydoop_exception("jc_configure_float: no default for option '%s'" % k)
  setattr(obj, f, v)


def __cleanup_file_path(path):
  while path.startswith(os.path.sep):
    path = path[1:]
  return os.path.join(os.path.sep, path)


def make_input_split(filename, offset, length):
  """
  Make a fake (i.e. not tied to a real file) input split. Mostly
  useful for testing purposes.

  @param filename: file name.
  @param offset: byte offset of the split with respect to the
    beginning of the file
  @param length: length of the split in bytes
  """
  l = len(filename)
  s = pack(">h", l)
  s += filename
  s += pack(">q", offset)
  s += pack(">q", length)
  return s


def split_hdfs_path(path):
  """
  Split HDFS URLs into (hostname, port, path) tuples.

  >>> split_hdfs_path('hdfs://foobar.foo.com:1234/foodir/barfile')
  ('foobar.foo.com', 1234, '/foodir/barfile')
  >>> split_hdfs_path('file:///foodir/barfile')
  ('', 0, '/foodir/barfile')
  >>> split_hdfs_path('hdfs:///foodir/barfile')
  ('localhost', 0, '/foodir/barfile')

  @param path: a full hdfs path C{hdfs://<HOST>:<PORT>/some/path}
  """
  r = urlparse(path)
  if r.scheme == 'file':
    if r.netloc != '':
      raise_pydoop_exception('split_hdfs_path: illegal hdfs path <%s>' % path)
    return '', 0, r.path
  if r.scheme != 'hdfs':
    raise_pydoop_exception('split_hdfs_path: illegal hdfs path <%s>' % path)
  npath = 'http:' + path[len('hdfs:'):]
  r = urlparse(npath)
  if r.netloc is '':
    return 'localhost', 0, __cleanup_file_path(r.path)
  parts = r.netloc.split(':')
  if len(parts) == 2:
    return parts[0], int(parts[1]), r.path
  else:
    return parts[0], DEFAULT_HDFS_PORT, r.path
