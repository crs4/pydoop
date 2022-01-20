#!/usr/bin/env python

# BEGIN_COPYRIGHT
#
# Copyright 2009-2022 CRS4.
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

"""\
Stub of an application where each task is assigned an int range as a (start,
stop) tuple. The record reader feeds numbers from the specified range to the
mapper, which in this case does nothing but generate a random string. Besides
random data generation (e.g., for terasort), this could be used to assign a
subset of files from an HDFS directory to each mapper (e.g., for image
recognition).
"""

from __future__ import division

import uuid

import pydoop.mapreduce.api as api
import pydoop.mapreduce.pipes as pipes

# py2 compat
try:
    range = xrange
except NameError:
    pass


class Reader(api.RecordReader):

    def __init__(self, context):
        super(Reader, self).__init__(context)
        start, stop = context.input_split.payload
        self.gen = iter(range(start, stop))
        self.nitems = max(stop - start, 0)
        self.key = self.start = start

    def next(self):
        self.key = next(self.gen)
        return self.key, None

    def get_progress(self):
        done = self.key - self.start + 1
        return min(done / self.nitems, 1.0)


class Mapper(api.Mapper):

    def map(self, context):
        context.emit(context.key, uuid.uuid4().hex)


def __main__():
    pipes.run_task(pipes.Factory(Mapper, record_reader_class=Reader))


if __name__ == "__main__":
    __main__()
