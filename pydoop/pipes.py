# BEGIN_COPYRIGHT
# END_COPYRIGHT
"""
Provides the basic MapReduce task components (Mapper,
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
  """
  Maps input key/value pairs to a set of intermediate key/value pairs.
  """
  
  def __init__(self, context=None):
    super(Mapper, self).__init__()

  def map(self, context):
    """
    Called once for each key/value pair in the input
    split. Applications must override this, emitting an output (key,
    value) pair through the context.

    @param context: the MapContext object passed by the framework,
    used to get the input key/value pair and emit the output key/value
    pair.
    """
    raise NotImplementedError


class Reducer(BaseReducer):
  """
  Reduces a set of intermediate values which share a key to a
  (possibly) smaller set of values.
  """
  
  def __init__(self, context=None):
    super(Reducer, self).__init__()

  def reduce(self, context):
    """
    Called once for each key. Applications must override this, emitting an
    output (key, value) pair through the context.

    @param context: the ReduceContext object passed by the
    framework, used to get the input key and corresponding set of
    values and emit the output key/value pair.
    """
    raise NotImplementedError


class RecordReader(BaseRecordReader):
  """
  Breaks the data into key/value pairs for input to the C{Mapper}.
  """
  
  def __init__(self, context=None):
    super(RecordReader, self).__init__()

  def next(self):
    """
    Called by the framework to provide a key/value pair to the
    C{Mapper}. Applications must override this.

    @rtype: tuple
    @return: a tuple of three elements. The first one is a bool which
      is True if a record is being read and False otherwise (signaling
      the end of the input split). The second and third element are,
      respectively, the key and the value (as strings).
    """
    raise NotImplementedError

  def getProgress(self):
    """
    The current progress of the record reader through its
    data. Applications must override this.

    @rtype: float
    @return: the fraction of the data read as a number between 0.0 and 1.0.
    """
    raise NotImplementedError


class RecordWriter(BaseRecordWriter):
  """
  Writes the output <key, value> pairs to an output file.
  """
  def __init__(self, context=None):
    super(RecordWriter, self).__init__()

  def emit(self, key, value):
    """
    Writes a key/value pair. Applications must override this.

    @param key: a final output key.
    @param value: a final output value.
    """
    raise NotImplementedError


class Combiner(BaseCombiner):
  """
  Works exactly as a C{Reducer}, but values aggregation is performed
  locally to the machine hosting each map task.
  """
  def __init__(self, context=None):
    super(Combiner, self).__init__()

  def reduce(self, context):
    raise NotImplementedError


class Partitioner(BasePartitioner):
  """
  Controls the partitioning of the keys of the intermediate
  map-outputs. The key (or a subset of the key) is used to derive the
  partition, typically by a hash function. The total number of
  partitions is the same as the number of reduce tasks for the
  job. Hence this controls which of the C{m} reduce tasks the
  intermediate key (and hence the record) is sent for reduction.
  """
  
  def __init__(self, context=None):
    super(Partitioner, self).__init__()

  def partition(self, key, numOfReduces):
    """
    Get the partition number for a given key given the total number of
    partitions i.e. number of reduce-tasks for the job. Applications
    must override this.

    @param key: the key to be partioned.
    @param numOfReduces: the total number of reduces.

    @rtype: int
    @return: the partition number for C{key}.
    """
    raise NotImplementedError
