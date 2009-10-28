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

from pydoop.pipes import Mapper, Reducer, Factory, runTask
from pydoop.pipes import RecordReader, InputSplit, RecordWriter
from pydoop.pipes import Partitioner
from pydoop.utils import jc_configure, jc_configure_int

from pydoop.hdfs import hdfs
from pydoop.utils import split_hdfs_path


WORDCOUNT = "WORDCOUNT"
INPUT_WORDS = "INPUT_WORDS"
OUTPUT_WORDS = "OUTPUT_WORDS"


class WordCountMapper(Mapper):

  def __init__(self, context):
    super(WordCountMapper, self).__init__(context)
    self.inputWords = context.getCounter(WORDCOUNT, INPUT_WORDS)
    self.logger = logging.getLogger("Mapper")
  
  def map(self, context):
    k = context.getInputKey()
    self.logger.debug("key = %r" % struct.unpack(">q", k)[0])
    words = context.getInputValue().split()
    for w in words:
      context.emit(w, "1")
    context.incrementCounter(self.inputWords, len(words))


class WordCountReducer(Reducer):

  def __init__(self, context):
    super(WordCountReducer, self).__init__(context)
    self.outputWords = context.getCounter(WORDCOUNT, OUTPUT_WORDS)
  
  def reduce(self, context):
    s = 0
    while context.nextValue():
      s += int(context.getInputValue())
    context.emit(context.getInputKey(), str(s))
    context.incrementCounter(self.outputWords, 1)


class WordCountReader(RecordReader):
  """
  Mimics Hadoop's default LineRecordReader (keys are byte offsets with
  respect to the whole file; values are text lines).
  """
  def __init__(self, context):
    super(WordCountReader, self).__init__()
    self.logger = logging.getLogger("WordCountReader")
    self.isplit = InputSplit(context.getInputSplit())
    for a in "filename", "offset", "length":
      self.logger.debug("isplit.%s = %r" % (a, getattr(self.isplit, a)))
    self.host, self.port, self.fpath = split_hdfs_path(self.isplit.filename)
    self.fs = hdfs(self.host, self.port)
    self.file = self.fs.open_file(self.fpath, os.O_RDONLY, 0, 0, 0)
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


class WordCountWriter(RecordWriter):
  
  def __init__(self, context):
    super(WordCountWriter, self).__init__(context)
    jc = context.getJobConf()
    jc_configure_int(self, jc, "mapred.task.partition", "part")
    jc_configure(self, jc, "mapred.work.output.dir", "outdir")
    jc_configure(self, jc, "mapred.textoutputformat.separator", "sep", "\t")
    self.outfn = "%s/part-%05d" % (self.outdir, self.part)
    self.host, self.port, self.fpath = split_hdfs_path(self.outfn)
    self.fs = hdfs(self.host, self.port)
    self.file = self.fs.open_file(self.fpath, os.O_WRONLY, 0, 0, 0)

  def __del__(self):
    self.file.close()
    self.fs.close()

  def emit(self, key, value):
    self.file.write("%s%s%s\n" % (key, self.sep, value))


class WordCountPartitioner(Partitioner):

  def __init__(self, context):
    super(WordCountPartitioner, self).__init__(context)
    self.logger = logging.getLogger("Partitioner")

  def partition(self, key, numOfReduces):
    reducer_id = (hash(key) & sys.maxint) % numOfReduces
    self.logger.debug("reducer_id: %r" % reducer_id)
    return reducer_id


def main(argv):
  runTask(Factory(WordCountMapper, WordCountReducer,
                  record_reader_class=WordCountReader,
                  record_writer_class=WordCountWriter,
                  partitioner_class=WordCountPartitioner,
                  combiner_class=WordCountReducer
                  ))


if __name__ == "__main__":
  main(sys.argv)
