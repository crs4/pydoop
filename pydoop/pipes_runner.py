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

import os, tempfile
from pydoop.text_protocol import text_down_protocol
from pydoop.text_protocol import up_serializer


class pipes_runner(object):

  def __init__(self, program, output_visitor,
               down_protocol=text_down_protocol):
    self.program = program
    self.output_visitor = output_visitor
    fd, self.tmp_filename = tempfile.mkstemp(prefix="pydoop_")
    os.close(fd)
    self.down_channel = down_protocol(self.program, out_file=self.tmp_filename)
    # FIXME the following should be done with some metaclass magic...
    for n in ['start', 'abort',
              'set_job_conf', 'set_input_types',
              'run_map', 'run_reduce',
              'reduce_key', 'reduce_value', 'map_item']:
      self.__setattr__(n, self.down_channel.__getattribute__(n))

  def close(self):
    self.down_channel.close()
    with open(self.tmp_filename) as of:
      for l in of:
        l = l.strip()
        parts = l.split('\t')
        cmd = parts[0]
        f = self.output_visitor.__getattribute__(cmd)
        x = map(up_serializer.deserialize, parts[1:])
        f(*x)
    os.remove(self.tmp_filename)
