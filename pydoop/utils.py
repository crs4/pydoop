# BEGIN_COPYRIGHT
#
# Copyright 2009-2013 CRS4.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy
# of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.
#
# END_COPYRIGHT

"""
This module contains general utility functions for application writing.
"""

import logging, uuid
from struct import pack

import pydoop
pp = pydoop.import_version_specific_module('_pipes')

# for backwards compatibility
from pydoop.hdfs.path import split as split_hdfs_path


DEFAULT_LOG_LEVEL = "WARNING"


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


def jc_configure_log_level(obj, jc, k, f, df=None):
  """
  Works like :func:`jc_configure`\ , but converts ``jc[k]`` to a logging level.

  The default value, if specified, must be a log level string, e.g., 'INFO'.
  """
  jc_configure(obj, jc, k, f, df)
  a = getattr(obj, f)
  try:
    setattr(obj, f, getattr(logging, a))
  except AttributeError:
    raise ValueError("unsupported log level: %r" % a)


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


class NullHandler(logging.Handler):
  def emit(self, record):
    pass


class NullLogger(logging.Logger):
  def __init__(self):
    logging.Logger.__init__(self, "null")
    self.propagate = 0
    self.handlers = [NullHandler()]


def make_random_str(prefix="pydoop_", postfix=''):
  return "%s%s%s" % (prefix, uuid.uuid4().hex, postfix)
