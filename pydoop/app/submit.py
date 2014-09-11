#!/usr/bin/env python

# BEGIN_COPYRIGHT
#
# Copyright 2009-2014 CRS4.
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
An interface to simplify pydoop jobs submission.
"""

import os, sys, warnings, logging, argparse
logging.basicConfig(level=logging.INFO)

import pydoop
import pydoop.hdfs as hdfs
import pydoop.hadut as hadut
import pydoop.utils as utils
import pydoop.utils.conversion_tables as conv_tables


DEFAULT_REDUCE_TASKS = max(3 * hadut.get_num_nodes(offline=True), 1)
IS_JAVA_RR = "mapreduce.pipes.isjavarecordreader"
IS_JAVA_RW = "mapreduce.pipes.isjavarecordwriter"
INPUT_FORMAT = "mapreduce.pipes.inputformat"
OUTPUT_FORMAT = "mapreduce.pipes.outputformat"
CACHE_FILES = "mapreduce.job.cache.files"
USER_HOME = "mapreduce.admin.user.home.dir"
JOB_REDUCES = "mapreduce.job.reduces"
JOB_NAME = "mapreduce.job.name"
COMPRESS_MAP_OUTPUT = "mapreduce.map.output.compress"


class PydoopSubmitter(object):
  """Assemble and launches pydoop jobs. It supports both v1 and v2 mapreduce
  models and automatically adapts configuration variable names to the specific
  (1.x vs 2.x) hadoop version used.
  """

  DESCRIPTION = "Simplified pydoop jobs submission"

  def __init__(self):
    self.logger = logging.getLogger("PydoopSubmitter")
    self.properties = {
      CACHE_FILES: '',
      'mapred.create.symlink': 'yes', # this is for backward compatibility
      COMPRESS_MAP_OUTPUT : 'true',
      'bl.libhdfs.opts': '-Xmx48m'
      }
    self.args = None
    self.remote_wd = None
    self.remote_module = None
    self.remote_module_bn = None
    self.remote_exe = None
    self.pipes_code = None

  def set_args(self, args):
    """
    Configure job, based on the arguments provided.
    """
    self.logger.setLevel(getattr(logging, args.log_level))
    self.remote_exe = args.program
    self.properties[JOB_NAME] = args.job_name if args.job_name else 'pydoop'
    self.properties[IS_JAVA_RR] = 'false' if args.do_not_use_java_record_reader\
                                          else 'true'
    self.properties[IS_JAVA_RW] = 'false' if args.do_not_use_java_record_writer\
                                          else 'true'
    if args.input_format:
      self.properties[INPUT_FORMAT] = args.input_format
    if args.output_format:
      self.properties[OUTPUT_FORMAT] = args.output_format
    self.properties[JOB_REDUCES] = args.num_reducers    
    if args.job_name:
      self.properties[JOB_NAME] = args.job_name
    self.properties.update(dict(args.D or []))
    self.properties.update(dict(args.job_conf or []))

    cfiles = [self.properties[CACHE_FILES]] if self.properties[CACHE_FILES]\
                                            else []
    if args.cache:
      cfiles += args.cache
    if args.python_egg:
      cfiles += args.python_egg
    self.properties[CACHE_FILES] = ','.join(cfiles)
    self.args = args

  def __warn_user_if_wd_maybe_unreadable(self, abs_remote_path):
    """
    Check directories above the remote module and issue a warning if
    they are not traversable by all users.

    The reasoning behind this is mainly aimed at set-ups with a centralized
    Hadoop cluster, accessed by all users, and where the Hadoop task tracker
    user is not a superuser; an example may be if you're running a shared
    Hadoop without HDFS (using only a POSIX shared file system).  The task
    tracker correctly changes user to the job requester's user for most
    operations, but not when initializing the distributed cache, so jobs who
    want to place files not accessible by the Hadoop user into dist cache fail.
    """
    host, port, path = hdfs.path.split(abs_remote_path)
    if host == '' and port == 0: # local file system
      host_port = "file:///"
    else:
      # FIXME: this won't work with any scheme other than hdfs:// (e.g., s3)
      host_port = "hdfs://%s:%s/" % (host, port)
    path_pieces = path.strip('/').split(os.path.sep)
    fs = hdfs.hdfs(host, port)
    for i in xrange(0, len(path_pieces)):
      part = os.path.join(host_port, os.path.sep.join(path_pieces[0:i+1]))
      permissions = fs.get_path_info(part)['permissions']
      if permissions & 0111 != 0111:
        self.logger.warning(
          "the remote module %s may not be readable\n" +
          "by the task tracker when initializing the distributed cache.\n" +
          "Permissions on path %s: %s", abs_remote_path, part, oct(permissions))
        break

  def __generate_pipes_code(self):
    lines = []
    ld_path = os.environ.get('LD_LIBRARY_PATH', None)
    pypath = os.environ.get('PYTHONPATH', '')
    lines.append("#!/bin/bash")
    lines.append('""":"')

    if self.args.no_override_env:
      lines.append('exec "%s" -u "$0" "$@"' % 'python')
    else:
      if ld_path:
        lines.append('export LD_LIBRARY_PATH="%s"' % ld_path)
    
      if self.args.python_egg:
         pypath = ':'.join(self.args.python_egg + [pypath])
      if pypath:
        lines.append('export PYTHONPATH="%s"' % pypath)
      if (USER_HOME not in self.properties and
          'HOME' in os.environ and
          not self.args.no_override_home):
        lines.append('export HOME="%s"' % os.environ['HOME'])
      lines.append('exec "%s" -u "$0" "$@"' % sys.executable)
    lines.append('":"""')
    lines.append('from %s import main' % self.args.wrap)
    lines.append('main()')
    return os.linesep.join(lines) + os.linesep

  def __validate(self):
    if not hdfs.path.exists(self.args.input):
      raise RuntimeError("%r does not exist" % (self.args.input,))
    if hdfs.path.exists(self.args.output):
      raise RuntimeError("%r already exists" % (self.args.output,))

  def __clean_wd(self):
    if self.remote_wd:
      try:
        self.logger.debug(
          "Removing temporary working directory %s", self.remote_wd
          )
        hdfs.rmr(self.remote_wd)
      except IOError:
        pass


  def __setup_remote_paths(self):
    """
    Actually create the working directory and copy the module into it.

    Note: the script has to be readable by Hadoop; though this may not
    generally be a problem on HDFS, where the Hadoop user is usually
    the superuser, things may be different if our working directory is
    on a shared POSIX filesystem.  Therefore, we make the directory
    and the script accessible by all.
    """
    if not self.args.pretend:
      hdfs.mkdir(self.remote_wd)
      hdfs.chmod(self.remote_wd, "a+rx")
      if self.args.wrap:
          pipes_code = self.__generate_pipes_code()
          hdfs.dump(pipes_code, self.remote_exe)
      hdfs.chmod(self.remote_exe, "a+rx")
      self.__warn_user_if_wd_maybe_unreadable(self.remote_wd)
    self.logger.debug("Created%sremote paths:" % 
                      (' [simulation] ' if self.args.pretend else ' ')) 
    self.logger.debug(self.remote_wd)
    self.logger.debug(self.remote_exe)
    self.logger.debug(self.remote_module)
    if self.args.wrap:
      self.logger.debug('Generated pipes_code:\n\n')        
      self.logger.debug(self.__generate_pipes_code())        
      

  def run(self):
    if self.args is None:
      raise RuntimeError("cannot run without args, please call set_args")
    if self.args.mrv2:
      pydoop_jar = pydoop.jar_path()
      if pydoop_jar is None:
        raise RuntimeError("Can't find pydoop.jar, cannot switch to mrv2")
      submitter_class='it.crs4.pydoop.mapreduce.pipes.Submitter'
      classpath = pydoop_jar
    else:
      # FIXME we still need to handle the special case with hadoop security and local
      # file system.
      submitter_class='org.apache.hadoop.mapred.pipes.Submitter'
      classpath = None
    job_args = []
    if self.args.conf:
      job_args.extend(['-conf', self.args.conf.name])
    # handle input, output, jar, 
    job_args.extend(['-input', self.args.input])
    job_args.extend(['-output', self.args.output])
    job_args.extend(['-program', self.remote_exe])

    if not self.args.disable_property_name_conversion:
      ctable = conv_tables.mrv1_to_mrv2 \
               if self.args.mrv2 else conv_tables.mrv2_to_mrv1
      props = [(ctable.get(k, k), v) for (k, v) in self.properties.iteritems()]
      self.properties = dict(props)

    try:
      self.__setup_remote_paths()
      executor = hadut.run_class if not self.args.pretend \
                                 else self.fake_run_class
      executor(submitter_class, args=job_args, 
               properties=self.properties, classpath=classpath, 
               logger=self.logger)
      self.logger.info("Done")
    finally:
      self.__clean_wd()

  def fake_run_class(self, submitter_class, args, properties, classpath, logger):
    sys.stdout.write("hadut.run_class(%s, %s, %s, %s)\n" %
                      (submitter_class, args, properties, classpath))

def run(args):
  submitter = PydoopSubmitter()
  submitter.set_args(args)

  submitter.run()
  return 0

def kv_pair(s):
  return s.split("=", 1)

def add_parser_arguments(parser):
  parser.add_argument(
    'program', metavar='PROGRAM', help='the python mapreduce program',
  )
  parser.add_argument(
    'input', metavar='INPUT', help='input path to the maps',
  )
  parser.add_argument(
    'output', metavar='OUTPUT', help='output path from the reduces',
  )
  parser.add_argument(
    '--disable-property-name-conversion', action='store_true',
    help="Do not adapt property names to the hadoop version used."
    )
  parser.add_argument(
    '--mrv2', action='store_true',
    help="Use mapreduce v2 Hadoop Pipes framework. InputFormat and OutputFormat" +
    "classes should be v2 compliant."
    )
  parser.add_argument(
    '--do-not-use-java-record-reader', action='store_true',
    help="Disable java RecordReader"
    )
  parser.add_argument(
    '--do-not-use-java-record-writer', action='store_true',
    help="Disable java RecordWriter"
    )
  parser.add_argument(
    '--input-format', metavar='CLASS', type=str, 
    help="java classname of InputFormat." +
    "Default value depends on mapreduce version used."
  )
  parser.add_argument(
    '--output-format', metavar='CLASS', type=str, 
    help="java classname of OutputFormat." +
    "Default value depends on mapreduce version used."
  )
  parser.add_argument(
    '--num-reducers', metavar='INT', type=int, default=DEFAULT_REDUCE_TASKS,
    help="Number of reduce tasks. Specify 0 to only perform map phase"
    )
  parser.add_argument(
    '--no-override-home', action='store_true',
    help="Don't set the script's HOME directory to the $HOME in your " +
    "environment.  Hadoop will set it to the value of the " +
    "'%s' property" % USER_HOME
    )
  parser.add_argument(
    '--no-override-env', action='store_true',
    help="Use the default python executable and environment instead of " +
    "overriding HOME, LD_LIBRARY_PATH and PYTHONPATH"
    )
  parser.add_argument(
    '-D', metavar="NAME=VALUE", type=kv_pair, action="append",
    help='Set a Hadoop property, such as -D mapreduce.compress.map.output=true'
    )
  parser.add_argument(
    '--job-conf', metavar="NAME=VALUE", type=kv_pair, action="append", nargs='+',
    help='Set a Hadoop property, such as mapreduce.compress.map.output=true'
    )
  parser.add_argument(
    '--log-level', metavar="LEVEL", default="INFO", help="Logging level",
    choices=[ "DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL", "FATAL" ]
    )
  parser.add_argument(
    '--job-name', metavar='NAME', type=str, 
    help="This job name."
  )
  parser.add_argument(
    '--jar', metavar='JAR_FILE', type=str, action="append",
    help="Additional jar file"
  )
  parser.add_argument(
    '--python-egg', metavar='EGG_FILE', type=str, action="append",
    help="Additional python egg file"
  )
  parser.add_argument(
    '--conf', metavar='CONF_FILE', type=argparse.FileType('r'), 
    help="Configuration file"
  )
  parser.add_argument(
    '--cache', metavar='FILE', type=argparse.FileType('r'), action="append",
    help="Add this file to the distributed cache"
  )
  parser.add_argument(
    '--wrap', metavar='MODULE', type=str, 
    help="Wrap MODULE in a script with the appropriate launch environemnt. " +
    "It is assumed that MODULE is a python module with a main() method."  + 
    "The resulting pydoop program will be at written at the HDFS path defined by PROGRAM"
    )
  parser.add_argument(
    '--pretend', action='store_true',
    help="Do not actually submit a job, print the produced " +
    "configuration settings and the command line that would be invoked."
    )

def add_parser(subparsers):
  parser = subparsers.add_parser(
    "submit",
    description=PydoopSubmitter.DESCRIPTION,
    formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
  add_parser_arguments(parser)
  parser.set_defaults(func=run)
  return parser

