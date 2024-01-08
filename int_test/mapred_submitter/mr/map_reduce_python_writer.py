#!/usr/bin/env python

# BEGIN_COPYRIGHT
#
# Copyright 2009-2024 CRS4.
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

import pydoop.hdfs as hdfs
import pydoop.mapreduce.api as api
import pydoop.mapreduce.pipes as pipes

SEP_KEY = "mapreduce.output.textoutputformat.separator"


class Mapper(api.Mapper):

    def map(self, context):
        for w in context.value.split():
            context.emit(w, 1)


class Reducer(api.Reducer):

    def reduce(self, context):
        context.emit(context.key, sum(context.values))


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
        record_writer_class=Writer
    ))


if __name__ == "__main__":
    __main__()
