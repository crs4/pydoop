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


def split_hdfs_path(hdfs_url):
  """
  Split ``hdfs_url`` into a (hostname, port, path) tuple.

  :type hdfs_url: string
  :param hdfs_url: an HDFS url, e.g., ``hdfs://localhost:9000/user/me``
  :rtype: tuple
  :return: hostname, port, path
  """
  # Use a helper class to compile URL_PATTERN once and for all
  return _HdfsPathSplitter.split(hdfs_url)


class _HdfsPathSplitter(object):

  URL_PATTERN = re.compile(r"([a-z0-9+.-]+):(.*)")

  @classmethod
  def split(cls, hdfs_url):
    try:
      scheme, rest = cls.URL_PATTERN.match(hdfs_url).groups()
    except AttributeError:
      scheme, rest = "hdfs", hdfs_url
    if scheme == "hdfs":
      if rest[:2] == "//" and rest[2] != "/":
        try:
          netloc, path = rest[2:].split("/", 1)
        except ValueError:
          raise_pydoop_exception("%s is not a valid HDFS URL" % hdfs_url)
        path = "/%s" % path
        try:
          hostname, port = netloc.split(":")
        except ValueError:
          hostname, port = netloc, DEFAULT_PORT
        try:
          port = int(port)
        except ValueError:
          raise_pydoop_exception("port must be an integer")
      elif ":" in rest:
        raise_pydoop_exception("relative path in absolute URI")
      elif rest[0] != "/":
        path = "/user/%s/%s" % (os.environ["USER"], rest)
        hostname, port = "default", 0
      else:
        hostname, port, path = "default", 0, rest
    elif scheme == "file":
      hostname, port, path = "", 0, rest
    else:
      raise_pydoop_exception("unsupported scheme %r" % scheme)
    path = "/%s" % path.lstrip("/")  # not handled by normpath
    return hostname, port, os.path.normpath(path)
