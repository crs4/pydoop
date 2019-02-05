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

import sys
import time

import pydoop.hdfs as hdfs
import pydoop.mapreduce.api as api
import pydoop.mapreduce.pipes as pipes

SEP_KEY = "mapreduce.output.textoutputformat.separator"


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


class Writer(api.RecordWriter):

    def __init__(self, context):
        super(Writer, self).__init__(context)
        outfn = context.get_default_work_file()
        self.file = hdfs.open(outfn, "wt")
        self.sep = context.job_conf.get(SEP_KEY, "\t")

    def close(self):
        self.file.close()

    def emit(self, key, value):
        self.file.write(key + self.sep + str(value) + "\n")


def __main__():
    pipes.run_task(pipes.Factory(
        Mapper,
        reducer_class=Reducer,
        record_reader_class=Reader,
        record_writer_class=Writer
    ))


if __name__ == "__main__":
    __main__()
