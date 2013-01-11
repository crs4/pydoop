# BEGIN_COPYRIGHT
# 
# Copyright 2009-2013 CRS4.
# 
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy
# of the License at
# 
#   http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.
# 
# END_COPYRIGHT

"""
This module allows you to write the components of your MapReduce application.

The basic MapReduce components (Mapper, Reducer, RecordReader, etc.)
are provided as abstract classes. Application developers must subclass
them, providing implementations for all methods called by the
framework.
"""

import pydoop
pp = pydoop.import_version_specific_module('_pipes')


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

  def close(self):
    """
    Called after the mapper has finished its job.

    Overriding this method is **not** required.
    """
    pass


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

  def close(self):
    """
    Called after the reducer has finished its job.

    Overriding this method is **not** required.
    """
    pass


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

  def close(self):
    """
    Called after the record reader has finished its job.

    Overriding this method is **not** required.
    """
    pass


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

  def close(self):
    """
    Called after the record writer has finished its job.

    Overriding this method is **not** required.
    """
    pass


class Combiner(pp.Reducer):
  """
  Works exactly as a :class:`Reducer`\ , but values aggregation is performed
  locally to the machine hosting each map task.
  """
  def __init__(self, context=None):
    super(Combiner, self).__init__()

  def reduce(self, context):
    raise NotImplementedError

  def close(self):
    """
    Called after the combiner has finished its job.

    Overriding this method is **not** required.
    """
    pass


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


class Factory(pp.Factory):
  """
  Creates MapReduce application components.

  The classes to use for each component must be specified as arguments
  to the constructor.
  """
  def __init__(self, mapper_class, reducer_class,
               record_reader_class=None,
               record_writer_class=None,
               combiner_class=None,
               partitioner_class=None):
    pp.Factory.__init__(self)

    self.mapper_class  = mapper_class
    setattr(self, 'createMapper', self.__make_creator('mapper_class'))

    self.reducer_class  = reducer_class
    setattr(self, 'createReducer', self.__make_creator('reducer_class'))

    if record_reader_class is not None:
      self.record_reader_class = record_reader_class
      setattr(self, 'createRecordReader',
              self.__make_creator('record_reader_class'))

    if record_writer_class is not None:
      self.record_writer_class = record_writer_class
      setattr(self, 'createRecordWriter',
              self.__make_creator('record_writer_class'))

    if combiner_class is not None:
      self.combiner_class = combiner_class
      setattr(self, 'createCombiner',
              self.__make_creator('combiner_class'))

    if partitioner_class is not None:
      self.partitioner_class = partitioner_class
      setattr(self, 'createPartitioner',
              self.__make_creator('partitioner_class'))

  def __make_creator(self, name):
    cls = getattr(self, name)
    def __make_creator_helper(ctx):
      o = cls(ctx)
      return o
    return __make_creator_helper


def runTask(factory):
  """
  Run the assigned task in the framework.

  :param factory: a :class:`Factory` instance.
  :type factory: :class:`Factory`
  :rtype: bool
  :return: True, if the task succeeded.
  """
  return pp.runTask(factory)


class InputSplit(pp.input_split):
  """
  Represents the data to be processed by an individual :class:`Mapper`\ .

  Typically, it presents a byte-oriented view on the input and it is
  the responsibility of the :class:`RecordReader` to convert this to a
  record-oriented view.

  The ``InputSplit`` is a *logical* representation of the actual
  dataset chunk, expressed through the ``filename``, ``offset`` and
  ``length`` attributes.
  
  :param data: the byte string returned by :meth:`MapContext.getInputSplit`
  :type data: string
  """
  def __init__(self, data):
    super(InputSplit, self).__init__(data)
