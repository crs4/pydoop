"""
This module provides the basic MapReduce task components (Mapper,
Reducer, RecordReader, ...) as abstract classes. Application
developers must subclass them, providing implementations for all
methods called by the framework.
"""

from pydoop_pipes import runTask as runTask

from pydoop_pipes import Mapper  as BaseMapper
from pydoop_pipes import Reducer as BaseReducer
from pydoop_pipes import RecordReader as BaseRecordReader
from pydoop_pipes import RecordWriter as BaseRecordWriter
from pydoop_pipes import Reducer as BaseCombiner
from pydoop_pipes import Partitioner as BasePartitioner

from pydoop_pipes import TaskContext as TaskContext
from pydoop_pipes import MapContext as MapContext
from pydoop_pipes import ReduceContext as ReduceContext

from factory import Factory
from input_split import InputSplit


class Mapper(BaseMapper):
  
  def __init__(self, context=None):
    super(Mapper, self).__init__()

  def map(self, context):
    raise NotImplementedError


class Reducer(BaseReducer):
  
  def __init__(self, context=None):
    super(Reducer, self).__init__()

  def reduce(self, context):
    raise NotImplementedError


class RecordReader(BaseRecordReader):
  
  def __init__(self, context=None):
    super(RecordReader, self).__init__()

  def next(self):
    raise NotImplementedError

  def getProgress(self):
    raise NotImplementedError


class RecordWriter(BaseRecordWriter):
  
  def __init__(self, context=None):
    super(RecordWriter, self).__init__()

  def emit(self, key, value):
    raise NotImplementedError


class Combiner(BaseCombiner):
  
  def __init__(self, context=None):
    super(Combiner, self).__init__()

  def reduce(self, context):
    raise NotImplementedError


class Partitioner(BasePartitioner):
  
  def __init__(self, context=None):
    super(Partitioner, self).__init__()

  def partition(self, key, numOfReduces):
    raise NotImplementedError
