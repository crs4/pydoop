#!/usr/bin/env python

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

import struct

from pydoop.mapreduce.pipes import run_task, Factory
from pydoop.mapreduce.api import Mapper, Reducer


class WordCountMapper(Mapper):

    def map(self, context):
        for w in context.value.split():
            context.emit(w, 1)


class WordCountReducer(Reducer):

    def reduce(self, context):
        s = sum(context.values)
        context.emit(context.key.encode("utf-8"), struct.pack(">i", s))


def __main__():
    factory = Factory(WordCountMapper, WordCountReducer)
    run_task(factory, auto_serialize=False)
