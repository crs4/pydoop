# BEGIN_COPYRIGHT
# END_COPYRIGHT
"""
This module allows you to write the components of your MapReduce application.

The basic MapReduce components (Mapper, Reducer, RecordReader, etc.)
are provided as abstract classes. Application developers must subclass
them, providing implementations for all methods called by the
framework.
"""

import _pipes as pp
from factory import Factory
from input_split import InputSplit


class Mapper(pp.Mapper):
  """
  Maps input key/value pairs to a set of intermediate key/value pairs.
  """
  
  def __init__(self, context=None):
    super(Mapper, self).__init__()

  def map(self, context):
    """
    Called once for each key/value pair in the input
    split. Applications must override this, emitting an output
    key/value pair through the context.
    
    :param context: the :class:`MapContext` object passed by the
      framework, used to get the input key/value pair and emit the
      output key/value pair.
    """
    raise NotImplementedError


class Reducer(pp.Reducer):
  """
  Reduces a set of intermediate values which share a key to a
  (possibly) smaller set of values.
  """
  
  def __init__(self, context=None):
    super(Reducer, self).__init__()

  def reduce(self, context):
    """
    Called once for each key. Applications must override this, emitting an
    output key/value pair through the context.

    :param context: the :class:`ReduceContext` object passed by the framework,
      used to get the input key and corresponding set of values and
      emit the output key/value pair.
    """
    raise NotImplementedError


class RecordReader(pp.RecordReader):
  """
  Breaks the data into key/value pairs for input to the :class:`Mapper`\ .
  """
  
  def __init__(self, context=None):
    super(RecordReader, self).__init__()

  def next(self):
    """
    Called by the framework to provide a key/value pair to the
    :class:`Mapper`\ . Applications must override this.

    :rtype: tuple
    :return: a tuple of three elements. The first one is a bool which
      is True if a record is being read and False otherwise (signaling
      the end of the input split). The second and third element are,
      respectively, the key and the value (as strings).
    """
    raise NotImplementedError

  def getProgress(self):
    """
    The current progress of the record reader through its
    data. Applications must override this.

    :rtype: float
    :return: the fraction of data read up to now, as a float between 0 and 1.
    """
    raise NotImplementedError


class RecordWriter(pp.RecordWriter):
  """
  Writes the output key/value pairs to an output file.
  """
  def __init__(self, context=None):
    super(RecordWriter, self).__init__()

  def emit(self, key, value):
    """
    Writes a key/value pair. Applications must override this.

    :param key: a final output key
    :type key: string
    :param value: a final output value
    :type value: string
    """
    raise NotImplementedError


class Combiner(pp.Reducer):
  """
  Works exactly as a :class:`Reducer`\ , but values aggregation is performed
  locally to the machine hosting each map task.
  """
  def __init__(self, context=None):
    super(Combiner, self).__init__()

  def reduce(self, context):
    raise NotImplementedError


class Partitioner(pp.Partitioner):
  """
  Controls the partitioning of intermediate keys output by the
  :class:`Mapper`\ . The key (or a subset of it) is used to derive the
  partition, typically by a hash function. The total number of
  partitions is the same as the number of reduce tasks for the
  job. Hence this controls which of the ``m`` reduce tasks the
  intermediate key (and hence the record) is sent to for reduction.
  """
  
  def __init__(self, context=None):
    super(Partitioner, self).__init__()

  def partition(self, key, numOfReduces):
    """
    Get the partition number for ``key`` given the total number of
    partitions, i.e., the number of reduce tasks for the
    job. Applications must override this.

    :param key: the key of the key/value pair being dispatched
    :type key: string
    :param numOfReduces: the total number of reduces.
    :type numOfReduces: int
    :rtype: int
    :return: the partition number for ``key``\ .
    """
    raise NotImplementedError


def runTask(factory):
  """
  Run the assigned task in the framework.

  :param factory: a :class:`Factory` instance.
  :type factory: :class:`Factory`
  :rtype: bool
  :return: True, if the task succeeded.
  """
  return pp.runTask(factory)
