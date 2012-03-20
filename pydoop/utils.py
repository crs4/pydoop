# BEGIN_COPYRIGHT
# END_COPYRIGHT

"""
This module contains general utility functions for application writing.
"""

from struct import pack

import pydoop
pp = pydoop.import_version_specific_module('_pipes')

# for backwards compatibility
from pydoop.hdfs.path import split as split_hdfs_path


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
