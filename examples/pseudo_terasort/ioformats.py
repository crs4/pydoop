import pydoop.mapreduce.api as api
import pydoop.hdfs as hdfs
import logging

logging.basicConfig()
LOGGER = logging.getLogger("ioformats")
LOGGER.setLevel(logging.CRITICAL)

KEY_LENGTH = 10
# key + value + \n
RECORD_LENGTH = 91


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
        self.file.write(key + value + b'\n')


class Reader(api.RecordReader):
    """
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
            self.logger.debug("StopIteration on bad rec len %d", len(record))
            raise StopIteration
        self.bytes_read += RECORD_LENGTH
        # drop the final '\n'
        self.logger.debug('key: %s, val: %s',
                          record[:KEY_LENGTH], record[KEY_LENGTH:-1])
        return (record[:KEY_LENGTH], record[KEY_LENGTH:-1])

    def get_progress(self):
        return min(float(self.bytes_read) / self.isplit.length, 1.0)
