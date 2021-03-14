#!/usr/bin/env python

# BEGIN_COPYRIGHT
#
# Copyright 2009-2021 CRS4.
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
import pydoop.hdfs as hdfs


class Mapper(api.Mapper):

    def map(self, context):
        time.sleep(1)
        sys.stderr.write("processing: %r\n" % (context.value,))
        context.emit(context.key, len(context.value))


class Writer(api.RecordWriter):

    def __init__(self, context):
        super(Writer, self).__init__(context)
        jc = context.job_conf
        outfn = context.get_default_work_file()
        hdfs_user = jc.get("pydoop.hdfs.user", None)
        self.file = hdfs.open(outfn, "wt", user=hdfs_user)
        self.sep = jc.get("mapreduce.output.textoutputformat.separator", "\t")

    def close(self):
        self.file.close()

    def emit(self, key, value):
        self.file.write(str(key) + self.sep + str(value) + "\n")


FACTORY = pipes.Factory(Mapper, record_writer_class=Writer)


def __main__():
    pipes.run_task(FACTORY)
