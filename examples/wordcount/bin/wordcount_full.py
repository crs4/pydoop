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
LOGGER = logging.getLogger("WordCount")
LOGGER.setLevel(logging.INFO)

from hashlib import md5

import pydoop.mapreduce.api as api
import pydoop.mapreduce.pipes as pipes
import pydoop.hdfs as hdfs


class Mapper(api.Mapper):

    def __init__(self, context):
        super(Mapper, self).__init__(context)
        context.set_status("initializing mapper")
        self.input_words = context.get_counter("WORDCOUNT", "INPUT_WORDS")

    def map(self, context):
        words = context.value.split()
        for w in words:
            context.emit(w, 1)
        context.increment_counter(self.input_words, len(words))


class Reducer(api.Reducer):

    def __init__(self, context):
        super(Reducer, self).__init__(context)
        context.set_status("initializing reducer")
        self.output_words = context.get_counter("WORDCOUNT", "OUTPUT_WORDS")

    def reduce(self, context):
        context.emit(context.key, sum(context.values))
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
        key = self.isplit.offset + self.bytes_read
        record = self.file.readline()
        if not record:  # end of file
            raise StopIteration
        self.bytes_read += len(record)
        return (key, record.decode("utf-8"))

    def get_progress(self):
        return min(float(self.bytes_read) / self.isplit.length, 1.0)


class Writer(api.RecordWriter):

    def __init__(self, context):
        super(Writer, self).__init__(context)
        self.logger = LOGGER.getChild("Writer")
        jc = context.job_conf
        outfn = context.get_default_work_file()
        self.logger.info("writing to %s", outfn)
        hdfs_user = jc.get("pydoop.hdfs.user", None)
        self.file = hdfs.open(outfn, "wt", user=hdfs_user)
        self.sep = jc.get("mapreduce.output.textoutputformat.separator", "\t")

    def close(self):
        self.logger.debug("closing open handles")
        self.file.close()
        self.file.fs.close()

    def emit(self, key, value):
        self.file.write(key + self.sep + str(value) + "\n")


class Partitioner(api.Partitioner):

    def __init__(self, context):
        super(Partitioner, self).__init__(context)
        self.logger = LOGGER.getChild("Partitioner")

    def partition(self, key, n_reduces):
        reducer_id = int(md5(key).hexdigest(), 16) % n_reduces
        self.logger.debug("reducer_id: %r" % reducer_id)
        return reducer_id


# DOCS_INCLUDE_START
FACTORY = pipes.Factory(
    mapper_class=Mapper,
    reducer_class=Reducer,
    record_reader_class=Reader,
    record_writer_class=Writer,
    partitioner_class=Partitioner,
    combiner_class=Reducer
)
# DOCS_INCLUDE_END


def main():
    pipes.run_task(FACTORY)


if __name__ == "__main__":
    main()
