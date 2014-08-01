# BEGIN_COPYRIGHT
#
# Copyright 2009-2014 CRS4.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy
# of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.
#
# END_COPYRIGHT

"""
Pydoop API
==========

All computation is performed by instances of classes derived from Mapper,
Reducer and, possibly, Reader, Writer, Combiner and Partitioner.

All communication between the framework and these objects is mediated by
Context objects. That is:

 * when they are instantiated, they receive contextual
   information via a context object::

     def __init__(self, context):
         job_conf = context.job_conf
         input_split = context.input_split

 * when they are used, for instance invoking the map method of a mapper,
   they read data and emit back to the framework via a context object::

     def map(self, context):
         key, value = context.key, context.value
         ...
         context.emit(new_key, new_value)
"""

from abc import ABCMeta, abstractmethod


class PydoopError(Exception):
    pass


class Counter(object):
    """
    An interface to the Hadoop counters infrastructure.
    """

    def __init__(self, counter_id):
        self.id = counter_id

    def get_id(self):
        return self.id

    def getId(self):
        return self.get_id()


class JobConf(dict):
    """
    Configuration properties assigned to this job.
    """

    def __init__(self, values):
        if 1 & len(values):
            raise PydoopError('JobConf.__init__: len(values) should be even')
        super(JobConf, self).__init__(zip(values[::2], values[1::2]))

    def hasKey(self, key):
        return self.has_key(key)

    def get_int(self, key, default=None):
        return int(self.get(key, default))

    def getInt(self, key, default=None):
        return self.get_int(key, default)

    def get_float(self, key, default=None):
        return float(self.get(key, default))

    def getFloat(self, key, default=None):
        return self.get_float(key, default)

    def get_bool(self, key, default=None):
        return bool(self.get(key, default))

    def getBoolean(self, key, default=None):
        return self.get_bool(key, default)

    def get(self, k): #FIXME: deprecated behaviour, here only for backward compatibility
        try:
            return self[k]
        except KeyError as ex:
            raise RuntimeError(ex.message)

class Context(object):
    """
    Information inter-exchange object.

    Context instances are the only points of contact between the framework
    and user-defined code.
    """
    __metaclass__ = ABCMeta

    @property
    def job_conf(self):
        return self.get_job_conf()

    @abstractmethod
    def get_job_conf(self):
        pass

    def getJobConf(self):
        return self.get_job_conf()

    @property
    def key(self):
        return self.get_input_key()

    @abstractmethod
    def get_input_key(self):
        pass

    def getInputKey(self):
        return self.get_input_key()

    @property
    def value(self):
        return self.get_input_value()

    @abstractmethod
    def get_input_value(self):
        pass

    def getInputValue(self):
        return self.get_input_value()

    @abstractmethod
    def emit(self, key, value):
        pass

    @abstractmethod
    def progress(self):
        pass

    @abstractmethod
    def set_status(self, status):
        pass

    def setStatus(self, status):
        return self.set_status(status)

    @abstractmethod
    def get_counter(self, group, name):
        pass

    def getCounter(self, group, name):
        return self.get_counter(group, name)

    @abstractmethod
    def increment_counter(self, counter, amount):
        pass

    def incrementCounter(self, counter, amount):
        return self.increment_counter(counter, amount)


class MapContext(Context):
    @property
    def input_split(self):
        return self.get_input_split()

    @abstractmethod
    def get_input_split(self):
        pass

    def getInputSplit(self):
        return self.get_input_split()

    @property
    def input_key_class(self):
        return self.get_input_key_class()

    @abstractmethod
    def get_input_key_class(self):
        pass

    def getInputKeyClass(self):
        return self.get_input_key_class()

    @property
    def input_value_class(self):
        return self.get_input_value_class()

    @abstractmethod
    def get_input_value_class(self):
        pass

    def getInputValueClass(self):
        return self.get_input_value_class()


class ReduceContext(Context):
    @property
    def values(self):
        return self.get_input_values()

    @abstractmethod
    def get_input_values(self):
        pass

    def getInputValues(self):
        return self.get_input_values()

    @abstractmethod
    def next_value(self):
        pass

    def nextValue(self):
        return self.next_value()


class Closable(object):
    def close(self):
        """
        Called after the object has finished its job.

        Overriding this method is **not** required.
        """
        pass


class Mapper(Closable):
    """
    Maps input key/value pairs to a set of intermediate key/value pairs.
    """
    __metaclass__ = ABCMeta

    @abstractmethod
    def map(self, context):
        """
        Called once for each key/value pair in the input
        split. Applications must override this, emitting an output
        key/value pair through the context.

        :param context: the :class:`MapContext` object passed by the
          framework, used to get the input key/value pair and emit the
          output key/value pair.
        """
        assert isinstance(context, MapContext)


class Reducer(Closable):
    """
    Reduces a set of intermediate values which share a key to a
    (possibly) smaller set of values.
    """
    __metaclass__ = ABCMeta

    @abstractmethod
    def reduce(self, context):
        """
        Called once for each key. Applications must override this, emitting an
        output key/value pair through the context.

        :param context: the :class:`ReduceContext` object passed by the framework,
        used to get the input key and corresponding set of values and
        emit the output key/value pair.
        """
        assert isinstance(context, ReduceContext)


class Partitioner(object):
    """
    Controls the partitioning of intermediate keys output by the
    :class:`Mapper`\ . The key (or a subset of it) is used to derive the
    partition, typically by a hash function. The total number of
    partitions is the same as the number of reduce tasks for the
    job. Hence this controls which of the ``m`` reduce tasks the
    intermediate key (and hence the record) is sent to for reduction.
    """
    __metaclass__ = ABCMeta

    @abstractmethod
    def partition(self, key, num_of_reduces):
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
        assert isinstance(key, str)
        assert isinstance(num_of_reduces, int)


class RecordReader(Closable):
    """
    Breaks the data into key/value pairs for input to the :class:`Mapper`\ .
    """
    __metaclass__ = ABCMeta

    @abstractmethod
    def next(self):
        """
        Called by the framework to provide a key/value pair to the
        :class:`Mapper`\ . Applications must override this.
        It should raise a StopIteration
        exception when the data stream has been emptied..

        :rtype: tuple
        :return: a tuple of two elements. They are,
        respectively, the key and the value (as strings)

        """
        raise StopIteration

    @abstractmethod
    def get_progress(self):
        """
        The progress of the record reader through the split
        as a value between 0.0 and 1.0.
        """
        pass

    def getProgress(self):
        """
        The current progress of the record reader through its
        data. Applications must override this.

        :rtype: float
        :return: the fraction of data read up to now, as a float between 0 and 1.
        """
        return self.get_progress()


class RecordWriter(Closable):
    """
    Writes the output key/value pairs to an output file.
    """
    __metaclass__ = ABCMeta

    @abstractmethod
    def emit(self, key, value):
        """
        Writes a key/value pair. Applications must override this.

        :param key: a final output key
        :type key: string
        :param value: a final output value
        :type value: string
        """
        pass


class Factory(object):
    """
    Creates MapReduce application components.

    The classes to use for each component must be specified as arguments
    to the constructor.
    """
    __metaclass__ = ABCMeta

    @abstractmethod
    def create_mapper(self, context):
        assert isinstance(context, MapContext)

    @abstractmethod
    def create_reducer(self, context):
        assert isinstance(context, ReduceContext)

    def create_combiner(self, context):
        """
        Create a combiner object.

        Return the new combiner or None, if one is not needed.
        """
        assert isinstance(context, MapContext)
        return None

    def create_partitioner(self, context):
        """
        Create a partitioner object.

        Return the new partitioner or None, if the default partitioner
        should be used.
        """
        assert isinstance(context, MapContext)
        return None

    def create_record_reader(self, context):
        """
        Create a record reader object.

        Return the new record reader or None, if the Java record
        reader should be used.
        """
        assert isinstance(context, MapContext)
        return None

    def create_record_writer(self, context):
        """
        Create an application record writer.

        Return the new record writer or None, if the Java record
        writer should be used.
        """
        assert isinstance(context, ReduceContext)
        return None
