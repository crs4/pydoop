#!/bin/bash

""":"
export HADOOP_HOME=$HADOOP_PREFIX
export PYTHON_EGG_CACHE=/tmp/python_cache
exec /usr/bin/python -u $0 $@
":"""

import sys
import logging


logging.basicConfig()
logger = logging.getLogger("WordCount")
logger.setLevel(logging.CRITICAL)


import itertools as it
import pydoop.mapreduce.api as api
import pydoop.mapreduce.pipes as pp
from pydoop.utils.serialize import serialize_to_string


from pydoop.utils.misc import jc_configure, jc_configure_int
import pydoop.hdfs as hdfs
import re

WORDCOUNT = "WORDCOUNT"
INPUT_WORDS = "INPUT_WORDS"
OUTPUT_WORDS = "OUTPUT_WORDS"


class Mapper(api.Mapper):
    def __init__(self, context):
        super(Mapper, self).__init__(context)
        self.logger = logger.getChild("Mapper")
        context.set_status("initializing mapper")
        self.input_words = context.get_counter(WORDCOUNT, INPUT_WORDS)

    def map(self, context):
        k = context.key
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
        self.logger = logger.getChild("Reader")
        self.logger.debug('started')
        self.isplit = context.input_split
        for a in "filename", "offset", "length":
            self.logger.debug("isplit.{} = {}".format(
                    a, getattr(self.isplit, a)))
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
        self.logger = logging.getLogger("Writer")
        jc = context.getJobConf()
        jc_configure_int(self, jc, "mapred.task.partition", "part")
        jc_configure(self, jc, "mapred.work.output.dir", "outdir")
        jc_configure(self, jc, "mapred.textoutputformat.separator", "sep", "\t")
        jc_configure(self, jc, "pydoop.hdfs.user", "hdfs_user", None)
        self.outfn = "%s/part-%05d" % (self.outdir, self.part)
        self.file = hdfs.open(self.outfn, "w", user=self.hdfs_user)

    def close(self):
        self.logger.debug("closing open handles")
        self.file.close()
        self.file.fs.close()

    def emit(self, key, value):
        self.file.write("%s%s%s\n" % (key, self.sep, value))


class Partitioner(api.Partitioner):
    def __init__(self, context):
        super(Partitioner, self).__init__(context)
        self.logger = logging.getLogger("Partitioner")

    def partition(self, key, numOfReduces):
        reducer_id = (hash(key) & sys.maxint) % numOfReduces
        self.logger.debug("reducer_id: %r" % reducer_id)
        return reducer_id


if __name__ == "__main__":
    pp.run_task(pp.Factory(
        mapper_class=Mapper, reducer_class=Reducer,
        record_reader_class=Reader,
        record_writer_class=Writer,
        partitioner_class=Partitioner,
        combiner_class=Reducer
        ),
        private_encoding=True        
    )

