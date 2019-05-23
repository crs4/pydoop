# BEGIN_COPYRIGHT
#
# Copyright 2009-2019 CRS4.
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
Provides access to some functionalities available via the Hadoop shell.
"""

import os
import shlex
import subprocess

import pydoop.utils.misc as utils
import pydoop.hdfs as hdfs
from .utils.py3compat import basestring


# --- FIXME: perhaps we need a more sophisticated tool for setting args ---
GENERIC_ARGS = frozenset([
    "-conf", "-D", "-fs", "-jt", "-files", "-libjars", "-archives"
])

CSV_ARGS = frozenset([
    "-files", "-libjars", "-archives"
])


# generic args must go before command-specific args
def _pop_generic_args(args):
    generic_args = []
    i = len(args) - 1
    while i >= 0:
        if args[i] in GENERIC_ARGS:
            try:
                args[i + 1]
            except IndexError:
                raise ValueError("option %s has no value" % args[i])
            generic_args.extend(args[i: i + 2])
            del args[i: i + 2]
        i -= 1
    return generic_args


# -files f1 -files f2 --> -files f1,f2
def _merge_csv_args(args):
    merge_map = {}
    i = len(args) - 1
    while i >= 0:
        if args[i] in CSV_ARGS:
            try:
                args[i + 1]
            except IndexError:
                raise ValueError("option %s has no value" % args[i])
            k, v = args[i: i + 2]
            merge_map.setdefault(k, []).append(v.strip())
            del args[i: i + 2]
        i -= 1
    for k, vlist in merge_map.items():
        args.extend([k, ",".join(vlist)])

# FIXME: the above functions share a lot of code
# -------------------------------------------------------------------------


def _construct_property_args(prop_dict):
    return sum((['-D', '%s=%s' % p] for p in prop_dict.items()), [])


# inherits from RuntimeError for backwards compatibility
class RunCmdError(RuntimeError):
    """
    Raised by :func:`run_tool_cmd` and all functions that make
    use of it to indicate that the call failed (returned non-zero).
    """
    def __init__(self, returncode, cmd, output=None):
        RuntimeError.__init__(self, output)
        self.returncode = returncode
        self.cmd = cmd

    def __str__(self):
        m = RuntimeError.__str__(self)
        if m:
            return m  # mimic old run_cmd behaviour
        else:
            return "Command '%s' returned non-zero exit status %d" % (
                self.cmd, self.returncode
            )


# keep_streams must default to True for backwards compatibility
def run_tool_cmd(tool, cmd, args=None, properties=None, hadoop_conf_dir=None,
                 logger=None, keep_streams=True):
    """
    Run a Hadoop command.

    If ``keep_streams`` is set to :obj:`True` (the default), the
    stdout and stderr of the command will be buffered in memory.  If
    the command succeeds, the former will be returned; if it fails, a
    ``RunCmdError`` will be raised with the latter as the message.
    This mode is appropriate for short-running commands whose "result"
    is represented by their standard output (e.g.,
    ``rval = run_tool_cmd("hdfs", "dfsadmin", ["-safemode", "get"])``).

    If ``keep_streams`` is set to :obj:`False`, the command will write
    directly to the stdout and stderr of the calling process, and the
    return value will be empty.  This mode is appropriate for long
    running commands that do not write their "real" output to stdout.
    """
    if logger is None:
        logger = utils.NullLogger()
    _args = [tool]
    if hadoop_conf_dir:
        _args.extend(["--config", hadoop_conf_dir])
    _args.append(cmd)
    if properties:
        _args.extend(_construct_property_args(properties))
    if args:
        if isinstance(args, basestring):
            args = shlex.split(args)
        _merge_csv_args(args)
        gargs = _pop_generic_args(args)
        for seq in gargs, args:
            _args.extend(map(str, seq))
    logger.debug('final args: %r', (_args,))
    if keep_streams:
        p = subprocess.Popen(
            _args, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            universal_newlines=True,
        )
        output, error = p.communicate()
    else:
        p = subprocess.Popen(_args, stdout=None, stderr=None, bufsize=1)
        ret = p.wait()
        error = 'command exited with %d status' % ret if ret else ''
        output = ''
    if p.returncode:
        raise RunCmdError(p.returncode, ' '.join(_args), error)
    return output


def run_cmd(cmd, args=None, properties=None, hadoop_home=None,
            hadoop_conf_dir=None, logger=None, keep_streams=True):
    """
    Runs the ``hadoop`` command.

    Calls :func:`run_tool_cmd` with ``"hadoop"`` as the first argument.
    """
    return run_tool_cmd("hadoop", cmd, args=args, properties=properties,
                        hadoop_conf_dir=hadoop_conf_dir, logger=logger,
                        keep_streams=keep_streams)


def run_class(class_name, args=None, properties=None, classpath=None,
              hadoop_conf_dir=None, logger=None, keep_streams=True):
    """
    Run a Java class with Hadoop (equivalent of running ``hadoop
    <class_name>`` from the command line).

    Additional ``HADOOP_CLASSPATH`` elements can be provided via
    ``classpath`` (either as a non-string sequence where each element
    is a classpath element or as a ``':'``-separated string).  Other
    arguments are passed to :func:`run_cmd`.

    .. note::

      ``HADOOP_CLASSPATH`` makes dependencies available **only on the
      client side**.  If you are running a MapReduce application, use
      ``args=['-libjars', 'jar1,jar2,...']`` to make them available to
      the server side as well.
    """
    if logger is None:
        logger = utils.NullLogger()
    old_classpath = None
    if classpath:
        old_classpath = os.getenv('HADOOP_CLASSPATH', '')
        if isinstance(classpath, basestring):
            classpath = [classpath]
        # Prepend the classpaths provided by the user to the existing
        # HADOOP_CLASSPATH value.  Order matters.  We could work a little
        # harder to avoid duplicates, but it's not essential
        os.environ['HADOOP_CLASSPATH'] = ":".join(
            classpath + old_classpath.split(':', 1)
        )
        logger.debug('HADOOP_CLASSPATH: %r', os.getenv('HADOOP_CLASSPATH'))
    try:
        res = run_cmd(class_name, args, properties,
                      hadoop_conf_dir=hadoop_conf_dir, logger=logger,
                      keep_streams=keep_streams)
    finally:
        if old_classpath is not None:
            os.environ['HADOOP_CLASSPATH'] = old_classpath
    return res


def iter_mr_out_files(mr_out_dir):
    for fn in hdfs.ls(mr_out_dir):
        if hdfs.path.basename(fn).startswith("part"):
            yield fn


def collect_output(mr_out_dir, out_file=None):
    """
    Return all mapreduce output in ``mr_out_dir``.

    Append the output to ``out_file`` if provided.  Otherwise, return
    the result as a single string (it is the caller's responsibility to
    ensure that the amount of data retrieved fits into memory).
    """
    if out_file is None:
        output = []
        for fn in iter_mr_out_files(mr_out_dir):
            with hdfs.open(fn, "rt") as f:
                output.append(f.read())
        return "".join(output)
    else:
        block_size = 16777216
        with open(out_file, 'a') as o:
            for fn in iter_mr_out_files(mr_out_dir):
                with hdfs.open(fn) as f:
                    data = f.read(block_size)
                    while len(data) > 0:
                        o.write(data)
                        data = f.read(block_size)
