#!/usr/bin/env python

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

import pydoop.pipes as pp
import pydoop.utils as pu


class Mapper(pp.Mapper):

  def __init__(self, context):
    super(Mapper, self).__init__(context)
    context.setStatus("Initialization started")
    self.excluded_counter = context.getCounter("IPCOUNT", "EXCLUDED_LINES")
    jc = context.getJobConf()
    pu.jc_configure(self, jc, "ipcount.excludes", "excludes_fn", "")
    if self.excludes_fn:
      with open(self.excludes_fn) as f:
        self.excludes = set(l.strip() for l in f if not l.isspace())
    else:
      self.excludes = set()
    context.setStatus("Initialization done")

  def map(self, context):
    ip = context.getInputValue().split(None, 1)[0]
    if ip not in self.excludes:
      context.emit(ip, "1")
    else:
      context.incrementCounter(self.excluded_counter, 1)


class Reducer(pp.Reducer):

  def reduce(self, context):
    s = 0
    while context.nextValue():
      s += int(context.getInputValue())
    context.emit(context.getInputKey(), str(s))


if __name__ == "__main__":
  pp.runTask(pp.Factory(Mapper, Reducer, combiner_class=Reducer))
