# BEGIN_COPYRIGHT
#
# Copyright 2009-2019 CRS4.
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

"""\
This module provides the base abstract classes used to develop MapReduce
application components (:class:`Mapper`, :class:`Reducer`, etc.).
"""

import json
from abc import abstractmethod
from collections import namedtuple

from pydoop.utils.py3compat import ABC


# move to pydoop.properties?
AVRO_IO_MODES = {'k', 'v', 'kv', 'K', 'V', 'KV'}


class JobConf(dict):
    """
    Configuration properties assigned to this job.

    JobConf objects are instantiated by the framework and support the
    same interface as dictionaries, plus a few methods that perform
    automatic type conversion::

      >>> jc['a']
      '1'
      >>> jc.get_int('a')
      1
    """
    def get_int(self, key, default=None):
        """
        Same as :meth:`dict.get`, but the value is converted to an int.
        """
        value = self.get(key, default)
        return None if value is None else int(value)

    def get_float(self, key, default=None):
        """
        Same as :meth:`dict.get`, but the value is converted to a float.
        """
        value = self.get(key, default)
        return None if value is None else float(value)

    def get_bool(self, key, default=None):
        """
        Same as :meth:`dict.get`, but the value is converted to a bool.

        The boolean value is considered, respectively, :obj:`True` or
        :obj:`False` if the string is equal, ignoring case, to
        ``'true'`` or ``'false'``.
        """
        v = self.get(key, default)
        if v != default:
            v = v.strip().lower()
            if v == 'true':
                v = True
            elif v == 'false':
                v = False
            elif default is None:
                raise RuntimeError("invalid bool string: %s" % v)
            else:
                v = default
        return v

    def get_json(self, key, default=None):
        value = self.get(key, default)
        return None if value is None else json.loads(value)


class InputSplit(object):
    """\
    Represents a subset of the input data assigned to a single map task.

    ``InputSplit`` objects are created by the framework and made available
    to the user application via the ``input_split`` context attribute.
    """
    pass


class FileSplit(InputSplit,
                namedtuple("FileSplit", "filename, offset, length")):
    """\
    A subset (described by offset and length) of an input file.
    """
    pass


class OpaqueSplit(InputSplit, namedtuple("OpaqueSplit", "payload")):
    """\
    A wrapper for an arbitrary Python object.

    Opaque splits are created on the Python side before job submission,
    serialized as ``hadoop.io.Writable`` objects and stored in an HDFS file.
    The Java submitter reads the splits from the above file and forwards them
    to the Python tasks.

    .. note::

      Opaque splits are only available when running a job via ``pydoop
      submit``. The HDFS path where splits are stored is specified via the
      ``pydoop.mapreduce.pipes.externalsplits.uri`` configuration key.
    """
    pass


class Context(ABC):
    """
    Context objects are used for communication between the framework
    and the Mapreduce application.  These objects are instantiated by the
    framework and passed to user methods as parameters::

      class Mapper(api.Mapper):

          def map(self, context):
              key, value = context.key, context.value
              ...
              context.emit(new_key, new_value)
    """

    @property
    def input_split(self):
        """\
        The :class:`InputSplit` for this task (map tasks only).

        This tries to deserialize the raw split sent from upstream. In the
        most common scenario (file-based input format), the returned value
        will be a :class:`FileSplit`.

        To get the raw split, call :meth:`get_input_split` with ``raw=True``.
        """
        return self.get_input_split()

    @abstractmethod
    def get_input_split(self, raw=False):
        pass

    @property
    def job_conf(self):
        """
        MapReduce job configuration as a :class:`JobConf` object.
        """
        return self.get_job_conf()

    @abstractmethod
    def get_job_conf(self):
        pass

    @property
    def key(self):
        """
        Input key.
        """
        return self.get_input_key()

    @abstractmethod
    def get_input_key(self):
        pass

    @property
    def value(self):
        """
        Input value (map tasks only).
        """
        return self.get_input_value()

    @abstractmethod
    def get_input_value(self):
        pass

    @property
    def values(self):
        """
        Iterator over all values for the current key (reduce tasks only).
        """
        return self.get_input_values()

    @abstractmethod
    def get_input_values(self):
        pass

    @abstractmethod
    def emit(self, key, value):
        """
        Emit a key, value pair to the framework.
        """
        pass

    @abstractmethod
    def progress(self):
        pass

    @abstractmethod
    def set_status(self, status):
        """
        Set the current status.

        :type status: str
        :param status: a description of the current status
        """
        pass

    @abstractmethod
    def get_counter(self, group, name):
        """
        Get a :class:`Counter` from the framework.

        :type group: str
        :param group: counter group name
        :type name: str
        :param name: counter name

        The counter can be updated via :meth:`increment_counter`.
        """
        pass

    @abstractmethod
    def increment_counter(self, counter, amount):
        """
        Update a :class:`Counter` by the specified amount.
        """
        pass


class Closable(object):

    def close(self):
        """
        Called after the object has finished its job.

        Overriding this method is **not** required.
        """
        pass


class Component(ABC):

    def __init__(self, context):
        self.context = context


class Mapper(Component, Closable):
    """
    Maps input key/value pairs to a set of intermediate key/value pairs.
    """

    @abstractmethod
    def map(self, context):
        """
        Called once for each key/value pair in the input
        split. Applications must override this, emitting an output
        key/value pair through the context.

        :type context: :class:`Context`
        :param context: the context object passed by the
          framework, used to get the input key/value pair and emit the
          output key/value pair.
        """
        pass


class Reducer(Component, Closable):
    """
    Reduces a set of intermediate values which share a key to a
    (possibly) smaller set of values.
    """

    @abstractmethod
    def reduce(self, context):
        """
        Called once for each key. Applications must override this, emitting
        an output key/value pair through the context.

        :type context: :class:`Context`
        :param context: the context object passed by
          the framework, used to get the input key and corresponding
          set of values and emit the output key/value pair.
        """
        pass


class Combiner(Reducer):
    """\
    A ``Combiner`` performs the same actions as a :class:`Reducer`, but it
    runs locally within a map task. This helps cutting down the amount of data
    sent to reducers across the network, with the downside that map tasks
    require extra memory to cache intermediate key/value pairs. The cache size
    is controlled by ``"mapreduce.task.io.sort.mb"`` and defaults to 100 MB.

    Note that it's not strictly necessary to extend this class in order to
    write a combiner: all that's required is that it has the same interface as
    a :class:`reducer`. Indeed, in many cases it's useful to set the combiner
    class to be the same as the reducer class.
    """
    pass


class Partitioner(Component):
    r"""
    Controls the partitioning of intermediate keys output by the
    :class:`Mapper`\ . The key (or a subset of it) is used to derive the
    partition, typically by a hash function. The total number of
    partitions is the same as the number of reduce tasks for the
    job. Hence this controls which of the *m* reduce tasks the
    intermediate key (and hence the record) is sent to for reduction.
    """

    @abstractmethod
    def partition(self, key, num_of_reduces):
        r"""
        Get the partition number for ``key`` given the total number of
        partitions, i.e., the number of reduce tasks for the
        job. Applications must override this.

        :type key: str
        :param key: the key of the key/value pair being dispatched.
        :type numOfReduces: int
        :param numOfReduces: the total number of reduces.
        :rtype: int
        :return: the partition number for ``key``\ .
        """
        pass


class RecordReader(Component, Closable):
    r"""
    Breaks the data into key/value pairs for input to the :class:`Mapper`\ .
    """

    def __iter__(self):
        return self

    @abstractmethod
    def next(self):
        r"""
        Called by the framework to provide a key/value pair to the
        :class:`Mapper`\ . Applications must override this, making
        sure it raises :exc:`~exceptions.StopIteration` when there are no more
        records to process.

        :rtype: tuple
        :return: a tuple of two elements. They are, respectively, the
          key and the value (as strings)
        """
        raise StopIteration

    def __next__(self):
        return self.next()

    @abstractmethod
    def get_progress(self):
        """
        The current progress of the record reader through its data.

        :rtype: float
        :return: the fraction of data read up to now, as a float between 0
          and 1.
        """
        pass


class RecordWriter(Component, Closable):
    """
    Writes the output key/value pairs to an output file.
    """

    @abstractmethod
    def emit(self, key, value):
        """
        Writes a key/value pair. Applications must override this.

        :type key: str
        :param key: a final output key
        :type value: str
        :param value: a final output value
        """
        pass


class Factory(ABC):
    """\
    Creates MapReduce application components (e.g., mapper, reducer).

    A factory object must be created by the application and passed to the
    framework as the first argument to :func:`~.pipes.run_task`. All MapReduce
    applications need at least a mapper object, while other components are
    optional (the corresponding ``create_`` method can return :obj:`None`).
    Note that the reducer is optional only in map-only jobs, where the number
    of reduce tasks has been set to 0.

    :class:`~.pipes.Factory` provides a generic implementation that takes
    component *classes* as initialization parameters and creates component
    objects as needed.
    """

    @abstractmethod
    def create_mapper(self, context):
        pass

    def create_reducer(self, context):
        return None

    def create_combiner(self, context):
        """
        Create a combiner object.

        Return the new combiner or :obj:`None`, if one is not needed.
        """
        return None

    def create_partitioner(self, context):
        """
        Create a partitioner object.

        Return the new partitioner or :obj:`None`, if the default partitioner
        should be used.
        """
        return None

    def create_record_reader(self, context):
        """
        Create a record reader object.

        Return the new record reader or :obj:`None`, if the Java record
        reader should be used.
        """
        return None

    def create_record_writer(self, context):
        """
        Create an application record writer.

        Return the new record writer or :obj:`None`, if the Java record
        writer should be used.
        """
        return None
