# BEGIN_COPYRIGHT
#
# Copyright 2009-2015 CRS4.
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

"""
Avro tools.
"""

import logging
logging.basicConfig()
LOGGER = logging.getLogger('avrolib')
LOGGER.setLevel(logging.DEBUG)

# FIXME: avro is not a dependency
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter

from pydoop.mapreduce.api import RecordWriter, RecordReader
import pydoop.hdfs as hdfs


class SeekableDataFileReader(DataFileReader):

    FORWARD_WINDOW_SIZE = 8192

    def align_after(self, offset):
        """
        Search for a sync point after offset and align just after that.
        """
        f = self.reader
        if offset <= 0:  # FIXME what is a negative offset??
            f.seek(0)
            self.block_count = 0
            self._read_header()  # FIXME we can't extimate how big it is...
            return
        sm = self.sync_marker
        sml = len(sm)
        pos = offset
        while pos < self.file_length - sml:
            f.seek(pos)
            data = f.read(self.FORWARD_WINDOW_SIZE)
            sync_offset = data.find(sm)
            if sync_offset > -1:
                f.seek(pos + sync_offset)
                self.block_count = 0
                return
            pos += len(data)


#FIXME this is just an example with no error checking
class AvroReader(RecordReader):
    """
    Avro data file reader.

    Reads all data blocks that begin within the given input split.
    """
    def __init__(self, ctx):
        super(AvroReader, self).__init__(ctx)
        self.logger = LOGGER.getChild('AvroReader')
        isplit = ctx.input_split
        self.region_start = isplit.offset
        self.region_end = isplit.offset + isplit.length
        self.reader = SeekableDataFileReader(hdfs.open(isplit.filename),
                                             DatumReader())
        self.reader.align_after(isplit.offset)

    def next(self):
        pos = self.reader.reader.tell()
        if pos > self.region_end and self.reader.block_count == 0:
            raise StopIteration
        record = self.reader.next()
        return pos, record

    def get_progress(self):
        """
        Give a rough estimate of the progress done.
        """
        pos = self.reader.reader.tell()
        return min((pos - self.region_start)
                   / float(self.region_end - self.region_start),
                   1.0)


#FIXME this is just an example with no error checking
class AvroWriter(RecordWriter):

    schema = None

    def __init__(self, context):
        super(AvroWriter, self).__init__(context)
        self.logger = LOGGER.getChild('AvroWriter')
        job_conf = context.job_conf
        part = int(job_conf['mapreduce.task.partition'])
        outdir = job_conf["mapreduce.task.output.dir"]
        outfn = "%s/part-%05d" % (outdir, part)
        wh = hdfs.open(outfn, "w")
        self.logger.debug('created hdfs file %s', outfn)
        self.writer = DataFileWriter(wh, DatumWriter(), self.schema)
        self.logger.debug('opened AvroWriter')

    def close(self):
        self.writer.close()
        # FIXME do we really need to explicitely close the filesystem?
        self.writer.writer.fs.close()
        self.logger.debug('closed AvroWriter')
