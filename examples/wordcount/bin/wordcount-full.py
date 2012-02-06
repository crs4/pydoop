#!/usr/bin/env python

# BEGIN_COPYRIGHT
# END_COPYRIGHT

"""
A more comprehensive example that shows how to use most mapreduce and
hdfs features. The RecordReader, RecordWriter and Partitioner classes
shown here mimic the behavior of the default ones.
"""

import sys, os, logging, struct
logging.basicConfig(level=logging.DEBUG)

import pydoop.pipes as pp
from pydoop.utils import jc_configure, jc_configure_int
from pydoop.hadoop_utils import get_hadoop_version
import pydoop.hdfs as hdfs

HADOOP_HOME = os.getenv("HADOOP_HOME", "/opt/hadoop")
HADOOP_VERSION = get_hadoop_version(HADOOP_HOME)

WORDCOUNT = "WORDCOUNT"
INPUT_WORDS = "INPUT_WORDS"
OUTPUT_WORDS = "OUTPUT_WORDS"


class Mapper(pp.Mapper):

  def __init__(self, context):
    super(Mapper, self).__init__(context)
    context.setStatus("initializing")
    self.inputWords = context.getCounter(WORDCOUNT, INPUT_WORDS)
    self.logger = logging.getLogger("Mapper")
  
  def map(self, context):
    k = context.getInputKey()
    self.logger.debug("key = %r" % struct.unpack(">q", k)[0])
    words = context.getInputValue().split()
    for w in words:
      context.emit(w, "1")
    context.incrementCounter(self.inputWords, len(words))


class Reducer(pp.Reducer):

  def __init__(self, context):
    super(Reducer, self).__init__(context)
    context.setStatus("initializing")
    self.outputWords = context.getCounter(WORDCOUNT, OUTPUT_WORDS)
  
  def reduce(self, context):
    s = 0
    while context.nextValue():
      s += int(context.getInputValue())
    context.emit(context.getInputKey(), str(s))
    context.incrementCounter(self.outputWords, 1)


class Reader(pp.RecordReader):
  """
  Mimics Hadoop's default LineRecordReader (keys are byte offsets with
  respect to the whole file; values are text lines).
  """
  def __init__(self, context):
    super(Reader, self).__init__()
    self.fs = self.file = None
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


class Writer(pp.RecordWriter):
  
  def __init__(self, context):
    super(Writer, self).__init__(context)
    jc = context.getJobConf()
    if HADOOP_VERSION != (0,21,0):
      jc_configure_int(self, jc, "mapred.task.partition", "part")
      jc_configure(self, jc, "mapred.work.output.dir", "outdir")
      jc_configure(self, jc, "mapred.textoutputformat.separator", "sep", "\t")
    else:
      jc_configure_int(self, jc, "mapreduce.task.partition", "part")
      jc_configure(self, jc, "mapreduce.task.output.dir", "outdir")
      jc_configure(self, jc, "mapreduce.output.textoutputformat.separator",
                   "sep", "\t")
    self.outfn = "%s/part-%05d" % (self.outdir, self.part)
    self.file = hdfs.open(self.outfn, "w")

  def __del__(self):
    self.file.close()
    self.file.fs.close()

  def emit(self, key, value):
    self.file.write("%s%s%s\n" % (key, self.sep, value))


class Partitioner(pp.Partitioner):

  def __init__(self, context):
    super(Partitioner, self).__init__(context)
    self.logger = logging.getLogger("Partitioner")

  def partition(self, key, numOfReduces):
    reducer_id = (hash(key) & sys.maxint) % numOfReduces
    self.logger.debug("reducer_id: %r" % reducer_id)
    return reducer_id


if __name__ == "__main__":
    pp.runTask(pp.Factory(
      Mapper, Reducer,
      record_reader_class=Reader,
      record_writer_class=Writer,
      partitioner_class=Partitioner,
      combiner_class=Reducer
      ))
