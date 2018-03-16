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

import logging

logging.basicConfig()
LOGGER = logging.getLogger("MapOnly")
LOGGER.setLevel(logging.INFO)

import pydoop.mapreduce.api as api
import pydoop.mapreduce.pipes as pipes
import pydoop.hdfs as hdfs


class Mapper(api.Mapper):

    def __init__(self, context):
        self.name = hdfs.path.basename(context.input_split.filename)

    def map(self, context):
        context.emit((self.name, context.key), context.value.upper())


class Writer(api.RecordWriter):

    def __init__(self, context):
        super(Writer, self).__init__(context)
        self.logger = LOGGER.getChild("Writer")
        jc = context.job_conf
        outfn = context.get_default_work_file()
        self.logger.info("writing to %s", outfn)
        hdfs_user = jc.get("pydoop.hdfs.user", None)
        self.sep = jc.get("mapreduce.output.textoutputformat.separator", "\t")
        self.file = hdfs.open(outfn, "wt", user=hdfs_user)

    def close(self):
        self.file.close()
        self.file.fs.close()

    def emit(self, key, value):
        self.file.write("%r%s%s%s" % (key, self.sep, value, "\n"))


def __main__():
    pipes.run_task(pipes.Factory(
        mapper_class=Mapper,
        record_writer_class=Writer,
    ))


if __name__ == "__main__":
    __main__()
