# BEGIN_COPYRIGHT
#
# Copyright 2009-2015 CRS4.
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
Miscellaneous utilities.
"""

import logging
import sys
import time
import uuid
from struct import pack

logging.basicConfig()
LOGGER = logging.getLogger('base')
DEFAULT_LOG_LEVEL = "WARNING"


def get_logger(parent, name, level=logging.CRITICAL):
    """
    Get a logger object with the specified name and level.

    If ``parent`` is a logger, the returned logger will be a
    descendant to it.
    """
    try:
        logger = parent.getChild(name)
    except AttributeError:
        try:
            root = parent.root
        except AttributeError:  # parent is not a logger
            logger = logging.getLogger(name)
        else:  # old Python version, copy getChild code from Python 2.7
            if parent is not root:
                name = '.'.join((parent.name, name))
            logger = parent.manager.getLogger(name)
    logger.setLevel(level)
    return logger


def split_hdfs_path(hdfs_path, user=None):  # backwards compatibility
    from pydoop.hdfs.path import split
    return split(hdfs_path, user)


def raise_pydoop_exception(msg):  # backwards compatibility
    return UserWarning(msg)


def jc_configure(obj, jc, k, f, df=None):
    r"""
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
    :param df: default value for the attribute if ``k`` is not present in
      ``jc``
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
        raise_pydoop_exception(
            "jc_configure_int: no default for option '%s'" % k
        )
    setattr(obj, f, v)


def jc_configure_bool(obj, jc, k, f, df=None):
    r"""
    Works like :func:`jc_configure`\ , but converts ``jc[k]`` to a boolean.
    """
    v = df
    if jc.hasKey(k):
        v = jc.getBoolean(k)
    elif df is None:
        raise_pydoop_exception(
            "jc_configure_bool: no default for option '%s'" % k
        )
    setattr(obj, f, v)


def jc_configure_float(obj, jc, k, f, df=None):
    r"""
    Works like :func:`jc_configure`\ , but converts ``jc[k]`` to a float.
    """
    v = df
    if jc.hasKey(k):
        v = jc.getFloat(k)
    elif df is None:
        raise_pydoop_exception(
            "jc_configure_float: no default for option '%s'" % k
        )
    setattr(obj, f, v)


def jc_configure_log_level(obj, jc, k, f, df=None):
    r"""
    Works like :func:`jc_configure`\ , but converts ``jc[k]`` to a logging
    level.

    The default value, if specified, must be a log level string, e.g., 'INFO'.
    """
    jc_configure(obj, jc, k, f, df)
    a = getattr(obj, f)
    try:
        setattr(obj, f, getattr(logging, a))
    except AttributeError:
        raise ValueError("unsupported log level: %r" % a)


def make_input_split(filename, offset, length):
    r"""
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


class Timer(object):

    def __init__(self, ctx, counter_group=None):
        self.ctx = ctx
        self._start_times = {}
        self._counters = {}
        self._counter_group = counter_group if counter_group else "Timer"

    def _gen_counter_name(self, event):
        return "TIME_" + event.upper() + " (ms)"

    def _get_time_counter(self, name):
        if name not in self._counters:
            counter_name = self._gen_counter_name(name)
            self._counters[name] = self.ctx.getCounter(
                self._counter_group, counter_name
            )
        return self._counters[name]

    def start(self, s):
        self._start_times[s] = time.time()

    def stop(self, s):
        delta_ms = 1000 * (time.time() - self._start_times[s])
        self.ctx.incrementCounter(self._get_time_counter(s), int(delta_ms))

    def time_block(self, event_name):
        return self.TimingBlock(self, event_name)

    class TimingBlock(object):

        def __init__(self, timer, event_name):
            self._timer = timer
            self._event_name = event_name

        def __enter__(self):
            self._timer.start(self._event_name)
            return self._timer

        def __exit__(self, exception_type, exception_val, exception_tb):
            self._timer.stop(self._event_name)
            return False
