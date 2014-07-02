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

import sys, os, subprocess

import pydoop
pp = pydoop.import_version_specific_module('_pipes')


def _true_false_str(v):
  return 'true' if v else 'false'


class text_down_protocol(object):
  def __init__(self, pipes_program, out_file=None):
    if out_file:
      self.fd = open(out_file, "w")
    else:
      self.fd = sys.stdout
    self.pipes_program = os.path.realpath(pipes_program)
    self.proc = subprocess.Popen([self.pipes_program],
                                 stdin=subprocess.PIPE,
                                 stdout=self.fd)
  def __send(self, args):
    s = '\t'.join(args) + '\n'
    self.proc.stdin.write(s)

  def start(self):
    self.__send(['start',  '0'])

  def close(self):
    self.__send(['close'])
    self.proc.stdin.close()
    self.proc.wait()
    if self.fd is not sys.stdout:
      self.fd.close()

  def abort(self):
    self.__send(['abort'])
    self.proc.wait()
    if self.fd is not sys.stdout:
      self.fd.close()

  def set_job_conf(self, job_conf_dict):
    args = ['setJobConf', '%d' % (2*len(job_conf_dict))]
    for k, v in job_conf_dict.iteritems():
      args.append(k)
      args.append(v)
    self.__send(args)

  def set_input_types(self, key_type, value_type):
    self.__send(['setInputTypes', key_type, value_type])

  def run_map(self, input_split, num_reduces, piped_input=True):
    self.__send(['runMap', input_split, '%s' % num_reduces,
                 _true_false_str(piped_input)])

  def run_reduce(self, n=1, piped_output=True):
    self.__send(['runReduce', '%s' % n, _true_false_str(piped_output)])

  def reduce_key(self, k):
    self.__send(['reduceKey', k])

  def reduce_value(self, v):
    self.__send(['reduceValue', v])

  def map_item(self, k, v):
    self.__send(['mapItem', k, v])


class text_up_protocol(object):

  def __init__(self):
    pass

  def output(self, key, value):
    pass

  def partitioned_output(self, reduce_, key, value):
    pass

  def done(self):
    pass

  def progress(self, progress):
    pass

  def status(self, message):
    pass

  def register_counter(self, id_, group, name):
    pass

  def increment_counter(self, id_, amount):
    pass


class up_serializer(object):

  @staticmethod
  def deserialize(s):
    return pp.unquote_string(s)

  @staticmethod
  def serialize(s, delimiters):
    return pp.quote_string(s, delimiters)
