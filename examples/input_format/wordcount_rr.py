# BEGIN_COPYRIGHT
#
# Copyright 2009-2015 CRS4.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy
# of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.
#
# END_COPYRIGHT

import re

import pydoop.mapreduce.api as api
import pydoop.mapreduce.pipes as pp
import pydoop.hdfs as hdfs
from pydoop.utils.serialize import serialize_to_string


class Mapper(api.Mapper):

    def map(self, context):
        words = re.sub('[^0-9a-zA-Z]+', ' ', context.getInputValue()).split()
        for w in words:
            context.emit(w, 1)


class Reducer(api.Reducer):

    def reduce(self, context):
        s = sum(context.values)
        context.emit(context.key, s)


class Reader(api.RecordReader):
    """
    Mimics Hadoop's default LineRecordReader (keys are byte offsets with
    respect to the whole file; values are text lines).
    """
    def __init__(self, context):
        self.isplit = context.input_split
        self.file = hdfs.open(self.isplit.filename)
        self.file.seek(self.isplit.offset)
        self.bytes_read = 0
        if self.isplit.offset > 0:
            discarded = self.file.readline()
            self.bytes_read += len(discarded)

    def close(self):
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


factory = pp.Factory(mapper_class=Mapper, reducer_class=Reducer,
                     record_reader_class=Reader)


def __main__():
    pp.run_task(factory)
