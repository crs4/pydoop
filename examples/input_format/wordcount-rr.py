#!/usr/bin/env python

# BEGIN_COPYRIGHT
# END_COPYRIGHT

import logging, struct
logging.basicConfig(level=logging.DEBUG)

import pydoop.pipes as pp
import pydoop.hdfs as hdfs


class Mapper(pp.Mapper):
  
  def map(self, context):
    for w in context.getInputValue().split():
      context.emit(w, "1")


class Reducer(pp.Reducer):
  
  def reduce(self, context):
    s = 0
    while context.nextValue():
      s += int(context.getInputValue())
    context.emit(context.getInputKey(), str(s))


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
      discarded = self.file.readline()  # read by reader of previous split
      self.bytes_read += len(discarded)

  def __del__(self):
    self.file.close()
    self.file.fs.close()
    
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


if __name__ == "__main__":
  pp.runTask(pp.Factory(Mapper, Reducer,
                  record_reader_class=Reader))
