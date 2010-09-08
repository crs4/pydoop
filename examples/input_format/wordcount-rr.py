#!/usr/bin/env python

# BEGIN_COPYRIGHT
# END_COPYRIGHT

import sys, os, logging, struct
logging.basicConfig(level=logging.DEBUG)

from pydoop.pipes import Mapper, Reducer, Factory, runTask
from pydoop.pipes import RecordReader, InputSplit

from pydoop.hdfs import hdfs
from pydoop.utils import split_hdfs_path


class WordCountMapper(Mapper):
  
  def map(self, context):
    for w in context.getInputValue().split():
      context.emit(w, "1")


class WordCountReducer(Reducer):
  
  def reduce(self, context):
    s = 0
    while context.nextValue():
      s += int(context.getInputValue())
    context.emit(context.getInputKey(), str(s))


class WordCountReader(RecordReader):
  """
  Mimics Hadoop's default LineRecordReader (keys are byte offsets with
  respect to the whole file; values are text lines).
  """
  def __init__(self, context):
    super(WordCountReader, self).__init__()
    self.fs = self.file = None
    self.logger = logging.getLogger("WordCountReader")
    self.isplit = InputSplit(context.getInputSplit())
    for a in "filename", "offset", "length":
      self.logger.debug("isplit.%s = %r" % (a, getattr(self.isplit, a)))
    self.host, self.port, self.fpath = split_hdfs_path(self.isplit.filename)
    self.fs = hdfs(self.host, self.port)
    self.file = self.fs.open_file(self.fpath, os.O_RDONLY)
    self.logger.debug("readline chunk size = %r" % self.file.chunk_size)
    self.file.seek(self.isplit.offset)
    self.bytes_read = 0
    if self.isplit.offset > 0:
      discarded = self.file.readline()  # read by reader of previous split
      self.bytes_read += len(discarded)

  def __del__(self):
    self.file.close()
    self.fs.close()
    
  def next(self):
    if self.bytes_read > self.isplit.length:  # end of input split
      return (False, "", "")
    key = struct.pack(">q", self.isplit.offset+self.bytes_read)
    record = self.file.readline()
    if record == "":  # end of file
      return (False, "", "")
    self.bytes_read += len(record)
    return (True, key, record)

  def getProgress(self):
    return min(float(self.bytes_read)/self.isplit.length, 1.0)


def main(argv):
  runTask(Factory(WordCountMapper, WordCountReducer,
                  record_reader_class=WordCountReader))


if __name__ == "__main__":
  main(sys.argv)
