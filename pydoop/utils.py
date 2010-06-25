# BEGIN_COPYRIGHT
# END_COPYRIGHT
"""
This module contains general utility functions for application writing.
"""

import os
from urlparse import urlparse
from struct import pack
import pydoop_pipes as pp


def raise_pydoop_exception(msg):
  """
  Raise a generic Pydoop exception.

  The exception is built at the C++ level and translated into a
  :exc:`UserWarning` by the Boost wrapper.
  """
  return pp.raise_pydoop_exception(msg)


def jc_configure(obj, jc, k, f, df=None):
  """
  Gets a configuration parameter from ``jc`` and automatically sets a
  corresponding attribute on ``obj``\ .

  :type obj: any object, typically a MapReduce component
  :param obj: object on which the attribute must be set
  :type jc: :class:`JobConf`
  :param jc: a job configuration object
  :type k: string
  :param k: a configuration key
  :type f: string
  :param f: name of the attribute to set
  :type df: string
  :param df: default value for the attribute if ``k`` is not present in ``jc``
  """
  v = df
  if jc.hasKey(k):
    v = jc.get(k)
  elif df is None:
    raise_pydoop_exception("jc_configure: no default for option '%s'" % k)
  setattr(obj, f, v)


def jc_configure_int(obj, jc, k, f, df=None):
  """
  Works like :func:`jc_configure` , but converts ``jc[k]`` to an integer.
  """
  v = df
  if jc.hasKey(k):
    v = jc.getInt(k)
  elif df is None:
    raise_pydoop_exception("jc_configure_int: no default for option '%s'" % k)
  setattr(obj, f, v)


def jc_configure_bool(obj, jc, k, f, df=None):
  """
  Works like :func:`jc_configure`\ , but converts ``jc[k]`` to a boolean.
  """
  v = df
  if jc.hasKey(k):
    v = jc.getBoolean(k)
  elif df is None:
    raise_pydoop_exception("jc_configure_bool: no default for option '%s'" % k)
  setattr(obj, f, v)


def jc_configure_float(obj, jc, k, f, df=None):
  """
  Works like :func:`jc_configure`\ , but converts ``jc[k]`` to a float.
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
  Build a fake (i.e., not tied to a real file)
  :class:`InputSplit <pydoop.pipes.InputSplit>`\ . This is used for testing
  purposes.

  :type filename: string
  :param filename: file name
  :type offset: int
  :param offset: byte offset of the split with respect to the
    beginning of the file
  :type length: int
  :param length: length of the split in bytes
  """
  l = len(filename)
  s = pack(">h", l)
  s += filename
  s += pack(">q", offset)
  s += pack(">q", length)
  return s


def split_hdfs_path(hdfs_url):
  """
  Split ``hdfs_url`` into a (hostname, port, path) tuple.

  :type hdfs_url: string
  :param hdfs_url: an HDFS url, e.g., ``hdfs://localhost:9000/user/me``
  :rtype: tuple
  :return: hostname, port, path
  """
  e = 'Illegal HDFS url <%s>'
  r = urlparse(hdfs_url)
  if r.scheme == 'hdfs':
    parts = r.netloc.split(':')
    if len(parts) == 2:
      hostname, port, path = parts[0], int(parts[1]), r.path
    elif len(parts) == 1:
      hostname, port, path = parts[0] or "default", 0, r.path
    else:
      raise_pydoop_exception(e % hdfs_url)
  elif r.scheme == 'file':
    if r.netloc != '':
      raise_pydoop_exception(e % hdfs_url)
    hostname, port, path = '', 0, r.path
  elif r.scheme == '':
    first_chunk = r.path.lstrip("/").split("/", 1)[0]
    if ":" in first_chunk:
      raise_pydoop_exception(e % hdfs_url)
    if not r.path.startswith("/"):
      path = "/user/%s/%s" % (os.environ["USER"], r.path)
    else:
      path = r.path
    hostname, port = 'default', 0
  else:
    raise_pydoop_exception(e % hdfs_url)
  return hostname, port, os.path.normpath(path)
