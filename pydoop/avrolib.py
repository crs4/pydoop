from pydoop.mapreduce.api import RecordWriter, RecordReader
import pydoop.hdfs as hdfs

from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter

import logging
logging.basicConfig()
logger = logging.getLogger('avrolib')
logger.setLevel(logging.DEBUG)

class SeekableDataFileReader(DataFileReader):
    FORWARD_WINDOW_SIZE = 8192
    def align_after(self, offset):
        "Search for a sync point after offset and align just after that."
        f = self.reader
        if offset <= 0: # FIXME what is a negative offset??
            f.seek(0)
            self.block_count = 0
            self._read_header() # FIXME we can't extimate how big it is...
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
    """Read an avro data file. 
    It will read by record, all the data blocks that begin within the given isplit.
    """
    def __init__(self, ctx):
        self.logger = logger.getChild('AvroReader')
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
        "Rough estimate of the progress done."
        pos = self.reader.reader.tell()
        return min((pos - self.region_start)
                   / float(self.region_end - self.region_start),
                   1.0)

#FIXME this is just an example with no error checking
class AvroWriter(RecordWriter):
    schema = None
    def __init__(self, context):
        self.logger = logger.getChild('AvroWriter')
        job_conf = context.job_conf
        part   = int(job_conf['mapreduce.task.partition'])
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

