#!/usr/bin/env python

# BEGIN_COPYRIGHT
#
# Copyright 2009-2017 CRS4.
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

import os

import pydoop.mapreduce.api as api
import pydoop.mapreduce.pipes as pipes
import pydoop.hdfs as hdfs


class Mapper(api.Mapper):

    def map(self, context):
        context.emit(context.key, context.value.upper())


class Writer(api.RecordWriter):

    def __init__(self, context):
        super(Writer, self).__init__(context)
        jc = context.job_conf
        part = jc.get_int("mapreduce.task.partition")
        out_dir = jc["mapreduce.task.output.dir"]
        outfn = os.path.join(out_dir, "part-m-%05d" % part)
        hdfs_user = jc.get("pydoop.hdfs.user", None)
        self.sep = jc.get("mapreduce.output.textoutputformat.separator", "\t")
        self.file = hdfs.open(outfn, "wt", user=hdfs_user)

    def close(self):
        self.file.close()
        self.file.fs.close()

    def emit(self, key, value):
        self.file.write("%d%s%s%s" % (key, self.sep, value, os.linesep))


def __main__():
    pipes.run_task(pipes.Factory(
        mapper_class=Mapper,
        record_writer_class=Writer,
    ))
