#!/usr/bin/env python

# BEGIN_COPYRIGHT
#
# Copyright 2009-2019 CRS4.
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

from __future__ import division

import pydoop.hdfs as hdfs
import pydoop.mapreduce.api as api
import pydoop.mapreduce.pipes as pipes


class Mapper(api.Mapper):

    def map(self, context):
        for w in context.value.split():
            context.emit(w, 1)


class Reducer(api.Reducer):

    def reduce(self, context):
        context.emit(context.key, sum(context.values))


class Reader(api.RecordReader):

    def __init__(self, context):
        super(Reader, self).__init__(context)
        self.split = context.input_split
        self.file = hdfs.open(self.split.filename)
        self.bytes_read = 0
        if self.split.offset > 0:
            self.file.seek(self.split.offset)
            discarded = self.file.readline()  # handled in previous split
            self.bytes_read += len(discarded)

    def close(self):
        self.file.close()

    def next(self):
        if self.bytes_read > self.split.length:
            raise StopIteration
        key = self.split.offset + self.bytes_read
        value = self.file.readline()
        if not value:  # end of file
            raise StopIteration
        self.bytes_read += len(value)
        return key, value.decode("utf-8")

    def get_progress(self):
        return min(self.bytes_read / self.split.length, 1.0)


def __main__():
    pipes.run_task(pipes.Factory(
        Mapper,
        reducer_class=Reducer,
        record_reader_class=Reader
    ))


if __name__ == "__main__":
    __main__()
