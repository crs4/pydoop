#!/usr/bin/env python

# BEGIN_COPYRIGHT
#
# Copyright 2009-2020 CRS4.
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

import sys
import time

import pydoop.mapreduce.api as api
import pydoop.mapreduce.pipes as pipes


class Mapper(api.Mapper):

    def __init__(self, context):
        super(Mapper, self).__init__(context)
        self.t0 = time.time()

    def map(self, context):
        sys.stderr.write("in: %r, %r\n" % (context.key, context.value))
        time.sleep(1)
        for w in context.value.split():
            context.emit(w, 1)

    def close(self):
        sys.stderr.write("total time: %.3f s\n" % (time.time() - self.t0))


class Reducer(api.Reducer):

    def __init__(self, context):
        super(Reducer, self).__init__(context)
        self.t0 = time.time()

    def reduce(self, context):
        sys.stderr.write("input key: %r\n" % (context.key,))
        time.sleep(1)
        context.emit(context.key, sum(context.values))

    def close(self):
        sys.stderr.write("total time: %.3f s\n" % (time.time() - self.t0))


def __main__():
    pipes.run_task(pipes.Factory(
        Mapper,
        combiner_class=Reducer,
        reducer_class=Reducer,
    ))


if __name__ == "__main__":
    __main__()
