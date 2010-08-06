# BEGIN_COPYRIGHT
# END_COPYRIGHT
"""
This module contains general utility functions for application writing.
"""

import os, re
from struct import pack
import _pipes as pp


# as in org/apache/hadoop/hdfs/server/namenode/NameNode.java
DEFAULT_PORT = 8020


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


def make_input_split(filename, offset, length):
  """
  Build a fake (i.e., not tied to a real file)
  :class:`~pydoop.pipes.InputSplit`\ . This is used for testing.

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


def split_hdfs_path(hdfs_path):
  """
  Split ``hdfs_path`` into a (hostname, port, path) tuple.

  :type hdfs_path: string
  :param hdfs_path: an HDFS path, e.g., ``hdfs://localhost:9000/user/me``
  :rtype: tuple
  :return: hostname, port, path
  """
  # Use a helper class to compile URL_PATTERN once and for all
  return _HdfsPathSplitter.split(hdfs_path)


class _HdfsPathSplitter(object):

  PATTERN = re.compile(r"([a-z0-9+.-]+):(.*)")

  @classmethod
  def raise_bad_path(cls, hdfs_path, why=None):
    msg = "'%s' is not a valid HDFS path" % hdfs_path
    msg += " (%s)" % why if why else ""
    raise_pydoop_exception(msg)

  @classmethod
  def split(cls, hdfs_path):
    try:
      scheme, rest = cls.PATTERN.match(hdfs_path).groups()
    except AttributeError:
      scheme, rest = "hdfs", hdfs_path
    if scheme == "hdfs":
      if rest[:2] == "//" and rest[2] != "/":
        try:
          netloc, path = rest[2:].split("/", 1)
        except ValueError:
          cls.raise_bad_path(hdfs_path, "path part is empty")
        path = "/%s" % path
        try:
          hostname, port = netloc.split(":")
        except ValueError:
          hostname, port = netloc, DEFAULT_PORT
        try:
          port = int(port)
        except ValueError:
          cls.raise_bad_path(hdfs_path, "port must be an integer")
      elif rest[0] != "/":
        path = "/user/%s/%s" % (os.environ["USER"], rest)
        hostname, port = "default", 0
      else:
        hostname, port, path = "default", 0, rest
      if ":" in path:
        cls.raise_bad_path(hdfs_path, "':' not allowed outside netloc part")
    elif scheme == "file":
      hostname, port, path = "", 0, rest
    else:
      cls.raise_bad_path(hdfs_path, "unsupported scheme %r" % scheme)
    path = "/%s" % path.lstrip("/")  # not handled by normpath
    return hostname, port, os.path.normpath(path)
