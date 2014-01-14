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
The hadut module provides access to some Hadoop functionalities
available via the Hadoop shell.
"""

import os, subprocess, shlex

import pydoop
import pydoop.utils as utils
import pydoop.hadoop_utils as hu
import pydoop.hdfs as hdfs


GLOB_CHARS = frozenset('*,?[]{}')

#--- FIXME: perhaps we need a more sophisticated tool for setting args ---
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
        args[i+1]
      except IndexError:
        raise ValueError("option %s has no value" % args[i])
      generic_args.extend(args[i:i+2])
      del args[i:i+2]
    i -= 1
  return generic_args

# -files f1 -files f2 --> -files f1,f2
def _merge_csv_args(args):
  merge_map = {}
  i = len(args) - 1
  while i >= 0:
    if args[i] in CSV_ARGS:
      try:
        args[i+1]
      except IndexError:
        raise ValueError("option %s has no value" % args[i])
      k, v = args[i:i+2]
      merge_map.setdefault(k, []).append(v.strip())
      del args[i:i+2]
    i -= 1
  for k, vlist in merge_map.iteritems():
    args.extend([k, ",".join(vlist)])

# FIXME: the above functions share a lot of code
#-------------------------------------------------------------------------


def _construct_property_args(prop_dict):
  return sum((['-D', '%s=%s' % p] for p in prop_dict.iteritems()), [])

class RunCmdError(RuntimeError): # inherits from RuntimeError for backwards compatibility
  """
  This exception is raised by run_cmd and all functions that make use of it to
  indicate that the call failed (returned non-zero).
  """
  def __init__(self, returncode, cmd, output=None):
    RuntimeError.__init__(self, output)
    self.returncode = returncode
    self.cmd = cmd

  def __str__(self):
    m = RuntimeError.__str__(self)
    if m:
      return m # mimic old run_cmd behaviour
    else:
      return "Command '%s' returned non-zero exit status %d" % (self.cmd, self.returncode)


def run_cmd(cmd, args=None, properties=None, hadoop_home=None,
            hadoop_conf_dir=None, logger=None):
  """
  Run a Hadoop command.

  If the command succeeds, return its output; if it fails, raise a
  ``RunCmdError`` with its error output as the message.

  .. code-block:: python

    >>> import uuid
    >>> properties = {'dfs.block.size': 32*2**20}
    >>> args = ['-put', 'hadut.py', uuid.uuid4().hex]
    >>> res = run_cmd('fs', args, properties)
    >>> res
    ''
    >>> print run_cmd('dfsadmin', ['-help', 'report'])
    -report: Reports basic filesystem information and statistics.
    >>> try:
    ...     run_cmd('foo')
    ... except RunCmdError as e:
    ...     print e
    ...
    Exception in thread "main" java.lang.NoClassDefFoundError: foo
    ...
  """
  if logger is None:
    logger = utils.NullLogger()
  hadoop = pydoop.hadoop_exec(hadoop_home=hadoop_home)
  _args = [hadoop]
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
  logger.info('args %s, cmd %s, properties %s', _args, cmd, properties)
  p = subprocess.Popen(_args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
  output, error = p.communicate()
  if p.returncode:
    raise RunCmdError(p.returncode, ' '.join(_args), error)
  return output


def get_task_trackers(properties=None, hadoop_conf_dir=None, offline=False):
  """
  Get the list of task trackers in the Hadoop cluster.

  Each element in the returned list is in the ``(host, port)`` format.
  ``properties`` is passed to :func:`run_cmd`.

  If ``offline`` is True, try getting the list of task trackers from
  the 'slaves' file in Hadoop's configuration directory (no attempt is
  made to contact the Hadoop daemons).  In this case, ports are set to 0.
  """
  if offline:
    if not hadoop_conf_dir:
      hadoop_conf_dir = pydoop.hadoop_conf()
      slaves = os.path.join(hadoop_conf_dir, "slaves")
    try:
      with open(slaves) as f:
        task_trackers = [(l.strip(), 0) for l in f]
    except IOError:
      task_trackers = []
  else:
    stdout = run_cmd("job", ["-list-active-trackers"],
                     properties=properties, hadoop_conf_dir=hadoop_conf_dir)
    task_trackers = []
    for l in stdout.splitlines():
      if not l:
        continue
      l = l.split(":")
      task_trackers.append((l[0].split("_")[1], int(l[-1])))
  return task_trackers


def get_num_nodes(properties=None, hadoop_conf_dir=None, offline=False):
  """
  Get the number of task trackers in the Hadoop cluster.

  ``properties`` is passed to :func:`get_task_trackers`.
  """
  return len(get_task_trackers(properties, hadoop_conf_dir, offline))


def dfs(args=None, properties=None, hadoop_conf_dir=None):
  """
  Run Hadoop dfs/fs.

  ``args`` and ``properties`` are passed to :func:`run_cmd`.
  """
  return run_cmd("dfs", args, properties, hadoop_conf_dir=hadoop_conf_dir)


def path_exists(path, properties=None, hadoop_conf_dir=None):
  """
  Return ``True`` if ``path`` exists in the default HDFS, else ``False``.

  ``properties`` is passed to :func:`dfs`.

  This function does the same thing as :func:`hdfs.path.exists
  <pydoop.hdfs.path.exists>`, but it uses a wrapper for the Hadoop
  shell rather than the hdfs extension.
  """
  try:
    dfs(["-stat", path], properties, hadoop_conf_dir=hadoop_conf_dir)
  except RuntimeError:
    return False
  return True


def run_jar(jar_name, more_args=None, properties=None, hadoop_conf_dir=None):
  """
  Run a jar on Hadoop (``hadoop jar`` command).

  ``more_args`` (after prepending ``jar_name``) and ``properties`` are
  passed to :func:`run_cmd`.

  .. code-block:: python

    >>> import glob, pydoop
    >>> hadoop_home = pydoop.hadoop_home()
    >>> v = pydoop.hadoop_version_info()
    >>> if v.cdh >= (4, 0, 0): hadoop_home += '-0.20-mapreduce'
    >>> jar_name = glob.glob('%s/*examples*.jar' % hadoop_home)[0]
    >>> more_args = ['wordcount']
    >>> try:
    ...     run_jar(jar_name, more_args=more_args)
    ... except RunCmdError as e:
    ...     print e
    ...
    Usage: wordcount <in> <out>
  """
  if hu.is_readable(jar_name):
    args = [jar_name]
    if more_args is not None:
      args.extend(more_args)
    return run_cmd("jar", args, properties, hadoop_conf_dir=hadoop_conf_dir)
  else:
    raise ValueError("Can't read jar file %s" % jar_name)


def run_class(class_name, args=None, properties=None, classpath=None,
              hadoop_conf_dir=None, logger=None):
  """
  Run a class that needs the Hadoop jars in its class path.

  ``args`` and ``properties`` are passed to :func:`run_cmd`.

  .. code-block:: python

    >>> cls = 'org.apache.hadoop.hdfs.tools.DFSAdmin'
    >>> print run_class(cls, args=['-help', 'report'])
    -report: Reports basic filesystem information and statistics.
  """
  if logger is None:
    logger = utils.NullLogger()
  old_classpath = None
  if classpath:
    old_classpath = os.getenv('HADOOP_CLASSPATH', '')
    if isinstance(classpath, basestring):
      classpath = [classpath]
    classpath_list = [cp.strip() for s in classpath for cp in s.split(":")]
    os.environ['HADOOP_CLASSPATH'] = ":".join(classpath_list)
    logger.debug('HADOOP_CLASSPATH %s',  os.environ['HADOOP_CLASSPATH'] )
  res = run_cmd(class_name, args, properties, hadoop_conf_dir=hadoop_conf_dir,
                logger=logger)
  if old_classpath is not None:
    os.environ['HADOOP_CLASSPATH'] = old_classpath
  return res


def run_pipes(executable, input_path, output_path, more_args=None,
              properties=None, force_pydoop_submitter=False,
              hadoop_conf_dir=None, logger=None):
  """
  Run a pipes command.

  ``more_args`` (after setting input/output path) and ``properties``
  are passed to :func:`run_cmd`.

  If not specified otherwise, this function sets the properties
  hadoop.pipes.java.recordreader and hadoop.pipes.java.recordwriter to 'true'.

  This function works around a bug in Hadoop pipes that affects versions of
  Hadoop with security when the local file system is used as the default FS
  (no HDFS); see https://issues.apache.org/jira/browse/MAPREDUCE-4000.
  In those set-ups, the function uses Pydoop's own pipes submitter application.
  You can force the use of Pydoop's submitter by passing the argument
  force_pydoop_submitter=True.
  """
  if logger is None:
    logger = utils.NullLogger()
  if not hdfs.path.exists(executable):
    raise IOError("executable %s not found" % executable)
  if not hdfs.path.exists(input_path) and not(set(input_path) & GLOB_CHARS):
    raise IOError("input path %s not found" % input_path)
  if properties is None:
    properties = {}
  properties.setdefault('hadoop.pipes.java.recordreader', 'true')
  properties.setdefault('hadoop.pipes.java.recordwriter', 'true')
  if force_pydoop_submitter:
    use_pydoop_submit = True
  else:
    use_pydoop_submit = False
    ver = pydoop.hadoop_version_info()
    if ver.has_security():
      if ver.cdh >= (4, 0, 0) and not ver.ext and hdfs.default_is_local():
        raise RuntimeError("mrv2 on local fs not supported yet")  # FIXME
      use_pydoop_submit = hdfs.default_is_local()
  args = [
    "-program", executable,
    "-input", input_path,
    "-output", output_path
    ]
  if more_args is not None:
    args.extend(more_args)
  if use_pydoop_submit:
    submitter = "it.crs4.pydoop.pipes.Submitter"
    pydoop_jar = pydoop.jar_path()
    args.extend(("-libjars", pydoop_jar))
    return run_class(submitter, args, properties, 
    classpath=pydoop_jar,
                     logger=logger)
  else:
    return run_cmd("pipes", args, properties, hadoop_conf_dir=hadoop_conf_dir,
                   logger=logger)


def find_jar(jar_name, root_path=None):
  """
  Look for the named jar in:

  #. ``root_path``, if specified
  #. working directory -- ``PWD``
  #. ``${PWD}/build``
  #. ``/usr/share/java``

  Return the full path of the jar if found; else return ``None``.
  """
  jar_name = os.path.basename(jar_name)
  root = root_path or os.getcwd()
  paths = (root, os.path.join(root, "build"), "/usr/share/java")
  for p in paths:
    p = os.path.join(p, jar_name)
    if os.path.exists(p):
      return p
  return None


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
      with hdfs.open(fn) as f:
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


class PipesRunner(object):
  """
  Allows to set up and run pipes jobs, optionally automating a few
  common tasks.

  :type prefix: string
  :param prefix: if specified, it must be a writable directory path
    that all nodes can see (the latter could be an issue if the local
    file system is used rather than HDFS)

  :type logger: :class:`logging.Logger`
  :param logger: optional logger

  If ``prefix`` is set, the runner object will create a working
  directory with that prefix and use it to store the job's input and
  output --- the intended use is for quick application testing.  If it
  is not set, you **must** call :meth:`set_output` with an hdfs path
  as its argument, and ``put`` will be ignored in your call to
  :meth:`set_input`.  In any event, the launcher script will be placed
  in the output directory's parent (this has to be writable for the
  job to succeed).
  """
  def __init__(self, prefix=None, logger=None):
    self.wd = self.exe = self.input = self.output = None
    self.logger = logger or utils.NullLogger()
    if prefix:
      self.wd = utils.make_random_str(prefix=prefix)
      hdfs.mkdir(self.wd)
      for n in "input", "output":
        setattr(self, n, hdfs.path.join(self.wd, n))

  def clean(self):
    """
    Remove the working directory, if any.
    """
    if self.wd:
      hdfs.rmr(self.wd)

  def set_input(self, input_, put=False):
    """
    Set the input path for the job.  If ``put`` is :obj:`True`, copy
    (local) ``input_`` to the working directory.
    """
    if put and self.wd:
      self.logger.info("copying input data to HDFS")
      hdfs.put(input_, self.input)
    else:
      self.input = input_
      self.logger.info("assigning input to %s" % self.input)

  def set_output(self, output):
    """
    Set the output path for the job.  Optional if the runner has been
    instantiated with a prefix.
    """
    self.output = output
    self.logger.info("assigning output to %s" % self.output)

  def set_exe(self, pipes_code):
    """
    Dump launcher code to the distributed file system.
    """
    if not self.output:
      raise RuntimeError("no output directory, can't create launcher")
    parent = hdfs.path.dirname(hdfs.path.abspath(self.output.rstrip("/")))
    self.exe = hdfs.path.join(parent, utils.make_random_str())
    hdfs.dump(pipes_code, self.exe)

  def run(self, **kwargs):
    """
    Run the pipes job.  Keyword arguments are passed to :func:`run_pipes`.
    """
    if not (self.input and self.output and self.exe):
      raise RuntimeError("setup incomplete, can't run")
    self.logger.info("running MapReduce application")
    run_pipes(self.exe, self.input, self.output, **kwargs)

  def collect_output(self, out_file=None):
    """
    Run :func:`collect_output` on the job's output directory.
    """
    self.logger.info("collecting output%s" % (
      " to %s" % out_file if out_file else ''
      ))
    self.logger.info("self.output %s", self.output)
    return collect_output(self.output, out_file)

  def __str__(self):
    res = [self.__class__.__name__]
    for n in "exe", "input", "output":
      res.append("  %s: %s" % (n, getattr(self, n)))
    return os.linesep.join(res) + os.linesep


class PydoopScriptRunner(PipesRunner):
  """
  Specialization of PipesRunner to support the set up and running of
  pydoop script jobs.
  """
  PYDOOP_EXE = None
  for path in os.environ['PATH'].split(os.pathsep):
    path = path.strip('"')
    exe_file = os.path.expanduser(os.path.join(path, 'pydoop'))
    if os.path.isfile(exe_file) and os.access(exe_file, os.X_OK):
      PYDOOP_EXE = exe_file
      break

  def run(self, script, more_args=None, pydoop_exe=PYDOOP_EXE):
    args = [pydoop_exe, "script", script, self.input, self.output]
    self.logger.info("running pydoop script")
    retcode = subprocess.call(args + (more_args or []))
    if retcode:
      raise RuntimeError("Error running pydoop_script")


if __name__ == "__main__":
  import doctest
  FLAGS = doctest.NORMALIZE_WHITESPACE | doctest.ELLIPSIS
  doctest.testmod(optionflags=FLAGS)
