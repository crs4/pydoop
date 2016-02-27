#!/bin/bash

# BEGIN_COPYRIGHT
#
# Copyright 2009-2016 CRS4.
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

""":"
export HADOOP_HOME=$HADOOP_PREFIX
export PYTHON_EGG_CACHE=/tmp/python_cache
exec /usr/bin/python -u $0 $@
":"""

import sys
import re
import logging

logging.basicConfig()
LOGGER = logging.getLogger("WordCount")
LOGGER.setLevel(logging.CRITICAL)

import pydoop.mapreduce.api as api
import pydoop.mapreduce.pipes as pp
from pydoop.utils.serialize import serialize_to_string
import pydoop.hdfs as hdfs

WORDCOUNT = "WORDCOUNT"
INPUT_WORDS = "INPUT_WORDS"
OUTPUT_WORDS = "OUTPUT_WORDS"


class Mapper(api.Mapper):

    def __init__(self, context):
        super(Mapper, self).__init__(context)
        self.logger = LOGGER.getChild("Mapper")
        context.set_status("initializing mapper")
        self.input_words = context.get_counter(WORDCOUNT, INPUT_WORDS)

    def map(self, context):
        words = re.sub('[^0-9a-zA-Z]+', ' ', context.value).split()
        for w in words:
            context.emit(w, 1)
        context.increment_counter(self.input_words, len(words))


class Reducer(api.Reducer):
    def __init__(self, context):
        super(Reducer, self).__init__(context)
        context.set_status("initializing reducer")
        self.output_words = context.get_counter(WORDCOUNT, OUTPUT_WORDS)

    def reduce(self, context):
        s = sum(context.values)
        context.emit(context.key, s)
        context.increment_counter(self.output_words, 1)


class Reader(api.RecordReader):
    """
    Mimics Hadoop's default LineRecordReader (keys are byte offsets with
    respect to the whole file; values are text lines).
    """
    def __init__(self, context):
        super(Reader, self).__init__(context)
        self.logger = LOGGER.getChild("Reader")
        self.logger.debug('started')
        self.isplit = context.input_split
        for a in "filename", "offset", "length":
            self.logger.debug(
                "isplit.{} = {}".format(a, getattr(self.isplit, a))
            )
        self.file = hdfs.open(self.isplit.filename)
        self.file.seek(self.isplit.offset)
        self.bytes_read = 0
        if self.isplit.offset > 0:
            discarded = self.file.readline()
            self.bytes_read += len(discarded)

    def close(self):
        self.logger.debug("closing open handles")
        self.file.close()
        self.file.fs.close()

    def next(self):
        if self.bytes_read > self.isplit.length:
            raise StopIteration
        key = serialize_to_string(self.isplit.offset + self.bytes_read)
        record = self.file.readline()
        if record == "":  # end of file
            raise StopIteration
        self.bytes_read += len(record)
        return (key, record)

    def get_progress(self):
        return min(float(self.bytes_read) / self.isplit.length, 1.0)


class Writer(api.RecordWriter):

    def __init__(self, context):
        super(Writer, self).__init__(context)
        self.logger = LOGGER.getChild("Writer")
        jc = context.job_conf
        part = jc.get_int("mapred.task.partition")
        out_dir = jc["mapred.work.output.dir"]
        outfn = "%s/part-%05d" % (out_dir, part)
        hdfs_user = jc.get("pydoop.hdfs.user", None)
        self.file = hdfs.open(outfn, "w", user=hdfs_user)
        self.sep = jc.get("mapred.textoutputformat.separator", "\t")

    def close(self):
        self.logger.debug("closing open handles")
        self.file.close()
        self.file.fs.close()

    def emit(self, key, value):
        self.file.write("%s%s%s\n" % (key, self.sep, value))


class Partitioner(api.Partitioner):

    def __init__(self, context):
        super(Partitioner, self).__init__(context)
        self.logger = LOGGER.getChild("Partitioner")

    def partition(self, key, num_reduces):
        reducer_id = (hash(key) & sys.maxint) % num_reduces
        self.logger.debug("reducer_id: %r" % reducer_id)
        return reducer_id


FACTORY = pp.Factory(
    mapper_class=Mapper,
    reducer_class=Reducer,
    record_reader_class=Reader,
    record_writer_class=Writer,
    partitioner_class=Partitioner,
    combiner_class=Reducer
)


def main():
    pp.run_task(FACTORY)

if __name__ == "__main__":
    main()


# Local Variables:
# mode: python
# End:
