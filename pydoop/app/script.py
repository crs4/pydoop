#!/usr/bin/env python

# BEGIN_COPYRIGHT
# 
# Copyright 2012 CRS4.
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
A quick and easy to use interface for running simple MapReduce jobs.
"""

import os, sys, warnings, logging
logging.basicConfig(level=logging.INFO)

import pydoop
import pydoop.hdfs as hdfs
import pydoop.hadut as hadut


PIPES_TEMPLATE = """
import sys, os
sys.path.insert(0, os.getcwd())

import pydoop.pipes
import %(module)s

class ContextWriter(object):

  def __init__(self, context):
    self.context = context
    self.counters = {}

  def emit(self, k, v):
    self.context.emit(str(k), str(v))

  def count(self, what, howmany):
    if self.counters.has_key(what):
      counter = self.counters[what]
    else:
      counter = self.context.getCounter('%(module)s', what)
      self.counters[what] = counter
    self.context.incrementCounter(counter, howmany)

  def status(self, msg):
    self.context.setStatus(msg)

  def progress(self):
    self.context.progress()

class PydoopScriptMapper(pydoop.pipes.Mapper):
  def __init__(self, ctx):
    super(type(self), self).__init__(ctx)
    self.writer = ContextWriter(ctx)

  def map(self, ctx):
    %(module)s.%(map_fn)s(ctx.getInputKey(), ctx.getInputValue(), self.writer)

class PydoopScriptReducer(pydoop.pipes.Reducer):

  def __init__(self, ctx):
    super(type(self), self).__init__(ctx)
    self.writer = ContextWriter(ctx)

  @staticmethod
  def iter(ctx):
    while ctx.nextValue():
      yield ctx.getInputValue()

  def reduce(self, ctx):
    key = ctx.getInputKey()
    %(module)s.%(reduce_fn)s(key, PydoopScriptReducer.iter(ctx), self.writer)

if __name__ == '__main__':
  result = pydoop.pipes.runTask(pydoop.pipes.Factory(
    PydoopScriptMapper, PydoopScriptReducer
    ))
  sys.exit(0 if result else 1)
"""


DEFAULT_REDUCE_TASKS = 3 * hadut.get_num_nodes()


def kv_pair(s):
  return s.split("=", 1)


class PydoopScript(object):

  DESCRIPTION = "Easy MapReduce scripting with Pydoop"

  def __init__(self):
    self.logger = logging.getLogger("PydoopScript")
    self.logger.setLevel(logging.DEBUG)  # TODO: expose as a cli param
    self.properties = {
      'hadoop.pipes.java.recordreader': 'true',
      'hadoop.pipes.java.recordwriter': 'true',
      'mapred.cache.files': '',
      'mapred.create.symlink': 'yes',
      'mapred.compress.map.output': 'true',
      'bl.libhdfs.opts': '-Xmx48m'
      }
    self.args = None
    self.runner = None

  def set_args(self, args):
    parent = hdfs.path.dirname(hdfs.path.abspath(args.output.rstrip("/")))
    prefix = hdfs.path.join(parent, "pydoop_script_")
    self.runner = hadut.PipesRunner(prefix=prefix, logger=self.logger)
    module_bn = os.path.basename(args.module)
    self.properties['mapred.job.name'] = module_bn
    self.properties.update(dict(args.D or []))
    self.properties['mapred.reduce.tasks'] = args.num_reducers
    self.properties['mapred.textoutputformat.separator'] = args.kv_separator
    remote_module = hdfs.path.join(self.runner.wd, module_bn)
    hdfs.put(args.module, remote_module)
    dist_cache_parameter = "%s#%s" % (remote_module, module_bn)
    if self.properties['mapred.cache.files']:
      self.properties['mapred.cache.files'] += ','
    self.properties['mapred.cache.files'] += dist_cache_parameter
    self.args = args

  def __generate_pipes_code(self):
    lines = []
    ld_path = os.environ.get('LD_LIBRARY_PATH', None)
    pypath = os.environ.get('PYTHONPATH', '')
    lines.append("#!/bin/bash")
    lines.append('""":"')
    if ld_path:
      lines.append('export LD_LIBRARY_PATH="%s"' % ld_path)
    if pypath:
      lines.append('export PYTHONPATH="%s"' % pypath)
    # override the script's home directory.
    if ("mapreduce.admin.user.home.dir" not in self.properties and
        'HOME' in os.environ and
        not self.args.no_override_home):
      lines.append('export HOME="%s"' % os.environ['HOME'])
    lines.append('exec "%s" -u "$0" "$@"' % sys.executable)
    lines.append('":"""')
    template_args = {
      'module': os.path.splitext(os.path.basename(self.args.module))[0],
      'map_fn': self.args.map_fn,
      'reduce_fn': self.args.reduce_fn,
      }
    lines.append(PIPES_TEMPLATE % template_args)
    return os.linesep.join(lines) + os.linesep

  def __validate(self):
    if not hdfs.path.exists(self.args.input):
      raise RuntimeError("%r does not exist" % (self.args.input,))
    if hdfs.path.exists(self.args.output):
      raise RuntimeError("%r already exists" % (self.args.output,))

  def run(self):
    if self.args is None:
      raise RuntimeError("cannot run without args, please call set_args")
    self.__validate()
    pipes_args = []
    if self.properties['mapred.textoutputformat.separator'] == '':
      pydoop_jar = pydoop.jar_path()
      if pydoop_jar is not None:
        self.properties[
          'mapred.output.format.class'
          ] = 'it.crs4.pydoop.NoSeparatorTextOutputFormat'
        pipes_args.extend(['-libjars', pydoop_jar])
      else:
        warnings.warn(
          "Can't find pydoop.jar, output will probably be tab-separated"
          )
    pipes_code = self.__generate_pipes_code()
    self.runner.set_input(self.args.input)
    self.runner.set_output(self.args.output)
    self.runner.set_exe(pipes_code)
    self.runner.run(more_args=pipes_args, properties=self.properties)


def run(args):
  script = PydoopScript()
  script.set_args(args)
  print script.run()
  return 0


def add_parser(subparsers):
  parser = subparsers.add_parser("script", description=PydoopScript.DESCRIPTION)
  parser.add_argument('module', metavar='MODULE', help='python module file')
  parser.add_argument('input', metavar='INPUT', help='hdfs input path')
  parser.add_argument('output', metavar='OUTPUT', help='hdfs output path')
  parser.add_argument('-m', '--map-fn', metavar='MAP', default='mapper',
                      help="name of map function within module")
  parser.add_argument('-r', '--reduce-fn', metavar='RED', default='reducer',
                      help="name of reduce function within module")
  parser.add_argument('-t', '--kv-separator', metavar='SEP', default='\t',
                      help="output key-value separator")
  parser.add_argument(
    '--num-reducers', metavar='INT', type=int, default=DEFAULT_REDUCE_TASKS,
    help="Number of reduce tasks. Specify 0 to only perform map phase"
    )
  parser.add_argument(
    '--no-override-home', action='store_true',
    help="Don't set the script's HOME directory to the $HOME in your " +
    "environment.  Hadoop will set it to the value of the " +
    "'mapreduce.admin.user.home.dir' property"
    )
  parser.add_argument(
    '-D', metavar="NAME=VALUE", type=kv_pair, action="append",
    help='Set a Hadoop property, such as -D mapred.compress.map.output=true'
    )
  parser.set_defaults(func=run)
  return parser
