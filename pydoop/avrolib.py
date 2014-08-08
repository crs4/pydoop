from pydoop.mapreduce.api import RecordWriter, RecordReader
import pydoop.hdfs as hdfs

from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter

class SeekableDataFileReader(DataFileReader):
    FORWARD_WINDOW_SIZE = 8192
    def align_after(self, offset):
        if offset <= 0: # FIXME Negative offsets??
            return
        f = self.reader.reader
        sm = self.reader.sync_marker
        pos = offset + len(sm)
        while pos < self.file_length:
            pos -=  len(sm)
            f.seek(pos)
            data = f.read(self.FORWARD_WINDOW_SIZE)
            sync_offset = data.find(sm)
            if sync_offset > -1:
                f.seek(pos + sync_offset)
                return
            pos += len(data)

class AvroReader(RecordReader):
    def __init__(self, ctx):
        isplit = ctx.input_split
        self.region_start = isplit.offset
        self.region_end = isplit.offset + isplit.length
        self.reader = SeekableDataFileReader(hdfs.open(isplit.filename),
                                             DatumReader())
        self.reader.align_after(isplit.offset)

    def next(self):
        pos = self.reader.reader.tell()
        if pos > self.region_end:
            raise StopIteration
        record = self.reader.next()
        return pos, record

    def get_progress(self):
        pos = self.reader.reader.tell()
        return min((pos - self.region_start)
                   / float(self.region_end - self.region_start),
                   1.0)

class AvroWriter(RecordWriter):
    schema = None
    def __init__(self, context):
        job_conf = context.job_conf
        part   = int(job_conf['mapred.task.partition'])
        outdir = job_conf["mapred.work.output.dir"]
        outfn = "%s/part-%05d" % (outdir, part)
        self.writer = DataFileWriter(hdfs.open(outfn, "w"),
                                     DatumWriter(), self.schema)

    def close(self):
        self.writer.close()
        # FIXME do we really need to explicitely close the filesystem?
        self.writer.writer.fs.close()

