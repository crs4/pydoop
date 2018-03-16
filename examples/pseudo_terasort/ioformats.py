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

import pydoop.mapreduce.api as api
import pydoop.hdfs as hdfs
import logging

logging.basicConfig()
LOGGER = logging.getLogger("ioformats")
LOGGER.setLevel(logging.WARNING)

KEY_LENGTH = 10
RECORD_LENGTH = 100  # key + value


class Writer(api.RecordWriter):

    def __init__(self, context):
        super(Writer, self).__init__(context)
        self.logger = LOGGER.getChild("Writer")
        jc = context.job_conf
        part = jc.get_int("mapred.task.partition")
        out_dir = jc["mapred.work.output.dir"]
        self.logger.debug("part: %d", part)
        self.logger.debug("outdir: %s", out_dir)
        outfn = "%s/part-%05d" % (out_dir, part)
        hdfs_user = jc.get("pydoop.hdfs.user", None)
        self.file = hdfs.open(outfn, "wb", user=hdfs_user)

    def close(self):
        self.logger.debug("closing open handles")
        self.file.close()
        self.file.fs.close()

    def emit(self, key, value):
        self.file.write(key + value)


class Reader(api.RecordReader):

    def __init__(self, context):
        super(Reader, self).__init__(context)
        self.logger = LOGGER.getChild("Reader")
        self.logger.debug('started')
        self.isplit = context.input_split
        for a in "filename", "offset", "length":
            self.logger.debug(
                "isplit.{} = {}".format(a, getattr(self.isplit, a))
            )
        remainder = self.isplit.offset % RECORD_LENGTH
        self.bytes_read = 0 if remainder == 0 else RECORD_LENGTH - remainder
        self.file = hdfs.open(self.isplit.filename)
        self.file.seek(self.isplit.offset + self.bytes_read)

    def close(self):
        self.logger.debug("closing open handles")
        self.file.close()
        self.file.fs.close()

    def next(self):
        if self.bytes_read > self.isplit.length:
            raise StopIteration
        record = self.file.read(RECORD_LENGTH)
        if not record:
            self.logger.debug("StopIteration on eof")
            raise StopIteration
        if len(record) < RECORD_LENGTH:  # possibly broken file?
            self.logger.warn("StopIteration on bad rec len %d", len(record))
            raise StopIteration
        self.bytes_read += RECORD_LENGTH
        self.logger.debug('key: %s, val: %s',
                          record[:KEY_LENGTH], record[KEY_LENGTH:])
        return (record[:KEY_LENGTH], record[KEY_LENGTH:])

    def get_progress(self):
        return min(float(self.bytes_read) / self.isplit.length, 1.0)


class CheckReader(Reader):

    def __init__(self, context):
        super(CheckReader, self).__init__(context)
        self.current_key = None

    def next(self):
        start_key, _ = super(CheckReader, self).next()
        okey = start_key
        try:
            while True:
                key, _ = super(CheckReader, self).next()
                assert key >= okey
                okey = key
        except StopIteration:
            return (self.isplit, [start_key, okey])
