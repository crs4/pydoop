#!/usr/bin/env python

# BEGIN_COPYRIGHT
#
# Copyright 2009-2018 CRS4.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy
# of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.
#
# END_COPYRIGHT

import pydoop.mapreduce.api as api
import pydoop.mapreduce.pipes as pp
from pydoop.avrolib import AvroContext


class Mapper(api.Mapper):
    def map(self, context):
        context.emit('', context.value['population'])


class Reducer(api.Reducer):
    def reduce(self, context):
        context.emit('', sum(context.values))


FACTORY = pp.Factory(Mapper, Reducer)
CONTEXT = AvroContext


def __main__():
    pp.run_task(FACTORY, private_encoding=True, context_class=CONTEXT)
