#!/bin/bash

# BEGIN_COPYRIGHT
#
# Copyright 2009-2014 CRS4.
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

"""
A comprehensive example that shows how to implement optional MapReduce
components and use the hdfs module. The RecordReader, RecordWriter and
Partitioner classes shown here mimic the behavior of the default ones.
"""

import sys
import logging
import struct

logging.basicConfig(level=logging.ERROR)

import re
import pydoop.pipes as pp
from pydoop.utils import jc_configure, jc_configure_int
import pydoop.hdfs as hdfs


WORDCOUNT = "WORDCOUNT"
INPUT_WORDS = "INPUT_WORDS"
OUTPUT_WORDS = "OUTPUT_WORDS"


class Mapper(pp.Mapper):

    def __init__(self, context):
        super(Mapper, self).__init__(context)
        self.logger = logging.getLogger("Mapper")
        context.setStatus("initializing")
        self.input_words = context.getCounter(WORDCOUNT, INPUT_WORDS)

    def map(self, context):
        words = re.sub('[^0-9a-zA-Z]+', ' ', context.getInputValue()).split()
        for w in words:
            context.emit(w, "1")
        context.incrementCounter(self.input_words, len(words))

    def close(self):
        self.logger.info("all done")


class Reducer(pp.Reducer):

    def __init__(self, context):
        super(Reducer, self).__init__(context)
        self.logger = logging.getLogger("Reducer")
        context.setStatus("initializing")
        self.output_words = context.getCounter(WORDCOUNT, OUTPUT_WORDS)

    def reduce(self, context):
        s = 0
        while context.nextValue():
            s += int(context.getInputValue())
        context.emit(context.getInputKey(), str(s))
        context.incrementCounter(self.output_words, 1)

    def close(self):
        self.logger.info("all done")


class Reader(pp.RecordReader):
    """
    Mimics Hadoop's default LineRecordReader (keys are byte offsets with
    respect to the whole file; values are text lines).
    """

    def __init__(self, context):
        super(Reader, self).__init__()
        self.logger = logging.getLogger("Reader")
        self.isplit = pp.InputSplit(context.getInputSplit())
        for a in "filename", "offset", "length":
            self.logger.debug("isplit.%s = %r" % (a, getattr(self.isplit, a)))
        self.file = hdfs.open(self.isplit.filename)
        self.logger.debug("readline chunk size = %r" % self.file.chunk_size)
        self.file.seek(self.isplit.offset)
        self.bytes_read = 0
        if self.isplit.offset > 0:
            # read by reader of previous split
            discarded = self.file.readline()
            self.bytes_read += len(discarded)

    def close(self):
        self.logger.debug("closing open handles")
        self.file.close()
        self.file.fs.close()

    def next(self):
        if self.bytes_read > self.isplit.length:  # end of input split
            return (False, "", "")
        key = struct.pack(">q", self.isplit.offset + self.bytes_read)
        record = self.file.readline()
        if record == "":  # end of file
            return (False, "", "")
        self.bytes_read += len(record)
        return (True, key, record)

    def getProgress(self):
        return min(float(self.bytes_read) / self.isplit.length, 1.0)


class Writer(pp.RecordWriter):
    def __init__(self, context):
        super(Writer, self).__init__(context)
        self.logger = logging.getLogger("Writer")
        jc = context.getJobConf()
        jc_configure_int(self, jc, "mapred.task.partition", "part")
        jc_configure(self, jc, "mapred.work.output.dir", "outdir")
        jc_configure(
            self, jc, "mapred.textoutputformat.separator", "sep", "\t"
        )
        jc_configure(self, jc, "pydoop.hdfs.user", "hdfs_user", None)
        self.outfn = "%s/part-%05d" % (self.outdir, self.part)
        self.file = hdfs.open(self.outfn, "w", user=self.hdfs_user)

    def close(self):
        self.logger.debug("closing open handles")
        self.file.close()
        self.file.fs.close()

    def emit(self, key, value):
        self.file.write("%s%s%s\n" % (key, self.sep, value))


class Partitioner(pp.Partitioner):

    def __init__(self, context):
        super(Partitioner, self).__init__(context)
        self.logger = logging.getLogger("Partitioner")

    def partition(self, key, numOfReduces):
        reducer_id = (hash(key) & sys.maxint) % numOfReduces
        self.logger.debug("reducer_id: %r" % reducer_id)
        return reducer_id


if __name__ == "__main__":
    pp.runTask(pp.Factory(
        Mapper, Reducer,
        record_reader_class=Reader,
        record_writer_class=Writer,
        partitioner_class=Partitioner,
        combiner_class=Reducer,
    ))

# Local Variables:
# mode: python
# End:
