# BEGIN_COPYRIGHT
#
# Copyright 2009-2018 CRS4.
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
The MapReduce API allows to write the components of a MapReduce application.

The basic MapReduce components (:class:`Mapper`, :class:`Reducer`,
:class:`RecordReader`, etc.)  are provided as abstract classes that
must be subclassed by the developer, providing implementations for all
methods called by the framework.
"""

import json
from abc import abstractmethod

from pydoop.utils.py3compat import ABC
from pydoop.utils.conversion_tables import mrv1_to_mrv2, mrv2_to_mrv1


class PydoopError(Exception):
    pass


class Counter(object):
    """
    An interface to the Hadoop counters infrastructure.

    Counter objects are instantiated and directly manipulated by the
    framework; users get and update them via the :class:`Context`
    interface.
    """

    def __init__(self, counter_id):
        self.id = counter_id

    def get_id(self):
        return self.id


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

    .. warning::

      For the most part, a JobConf object behaves like a :class:`dict`.
      For backwards compatibility, however, there are two important exceptions:

      #. objects are constructed from a ``[key1, value1, key2, value2,
         ...]`` sequence

      #. if ``k`` is not in ``jc``, ``jc.get(k)`` raises :exc:`RuntimeError`
         instead of returning :obj:`None` (``jc.get(k, None)`` returns
         :obj:`None` as in :class:`dict`).
    """

    def __init__(self, values):
        if 1 & len(values):
            raise PydoopError('JobConf.__init__: len(values) should be even')
        # FIXME -- this is a kludge but otherwise we would break backward
        # compatibility
        nvalues = [_.decode('UTF-8') if isinstance(_, bytes) else _
                   for _ in values]
        super(JobConf, self).__init__(zip(nvalues[::2], nvalues[1::2]))
        self.__mirror_conf_across_versions()

    def get_int(self, key, default=None):
        """
        Same as :meth:`dict.get`, but the value is converted to an int.
        """
        return int(self.get(key, default))

    def get_float(self, key, default=None):
        """
        Same as :meth:`dict.get`, but the value is converted to an float.
        """
        return float(self.get(key, default))

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
        return json.loads(self.get(key, default))

    # get below is deprecated behaviour, here only for backward compatibility
    def get(self, *args):
        if len(args) == 2:
            return super(JobConf, self).get(*args)
        else:
            try:
                return self[args[0]]
            except KeyError as ex:
                raise RuntimeError(ex.args[0])

    def __mirror_conf_across_versions(self):
        ext = {}
        for k in self:
            if k in mrv1_to_mrv2 and not mrv1_to_mrv2[k] in self:
                ext[mrv1_to_mrv2[k]] = self[k]
            if k in mrv2_to_mrv1 and not mrv2_to_mrv1[k] in self:
                ext[mrv2_to_mrv1[k]] = self[k]
        self.update(ext)


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
        Input value.
        """
        return self.get_input_value()

    @abstractmethod
    def get_input_value(self):
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


class MapContext(Context):
    """
    The context given to the mapper.
    """
    @property
    def input_split(self):
        """\
        The current input split as an :class:`~.pipes.InputSplit` object.
        """
        return self.get_input_split()

    @abstractmethod
    def get_input_split(self, raw=False):
        """\
        Get the current input split.

        If ``raw`` is :obj:`False` (the default), return an
        :class:`~.pipes.InputSplit` object; if it's :obj:`True`, return
        a byte string (the unserialized split as sent via the downlink).
        """
        pass

    @property
    def input_key_class(self):
        """
        Return the type of the input key.
        """
        return self.get_input_key_class()

    @abstractmethod
    def get_input_key_class(self):
        pass

    @property
    def input_value_class(self):
        return self.get_input_value_class()

    @abstractmethod
    def get_input_value_class(self):
        """
        Return the type of the input value.
        """
        pass


class ReduceContext(Context):
    """
    The context given to the reducer.
    """
    @property
    def values(self):
        return self.get_input_values()

    @abstractmethod
    def get_input_values(self):
        pass

    @abstractmethod
    def next_value(self):
        """
        Return :obj:`True` if there is another value that can be processed.
        """
        pass


class Closable(ABC):

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

    def __init__(self, context):
        self.context = context

    @abstractmethod
    def map(self, context):
        """
        Called once for each key/value pair in the input
        split. Applications must override this, emitting an output
        key/value pair through the context.

        :type context: :class:`MapContext`
        :param context: the context object passed by the
          framework, used to get the input key/value pair and emit the
          output key/value pair.
        """
        assert isinstance(context, MapContext)


class Reducer(Closable):
    """
    Reduces a set of intermediate values which share a key to a
    (possibly) smaller set of values.
    """

    def __init__(self, context=None):
        self.context = context

    @abstractmethod
    def reduce(self, context):
        """
        Called once for each key. Applications must override this, emitting
        an output key/value pair through the context.

        :type context: :class:`ReduceContext`
        :param context: the context object passed by
          the framework, used to get the input key and corresponding
          set of values and emit the output key/value pair.
        """
        assert isinstance(context, ReduceContext)


class Partitioner(ABC):
    r"""
    Controls the partitioning of intermediate keys output by the
    :class:`Mapper`\ . The key (or a subset of it) is used to derive the
    partition, typically by a hash function. The total number of
    partitions is the same as the number of reduce tasks for the
    job. Hence this controls which of the *m* reduce tasks the
    intermediate key (and hence the record) is sent to for reduction.
    """

    def __init__(self, context):
        self.context = context

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
        assert isinstance(key, str)
        assert isinstance(num_of_reduces, int)


class RecordReader(Closable):
    r"""
    Breaks the data into key/value pairs for input to the :class:`Mapper`\ .
    """

    def __init__(self, context=None):
        self.context = context

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


class RecordWriter(Closable):
    """
    Writes the output key/value pairs to an output file.
    """

    def __init__(self, context=None):
        self.context = context

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
    """
    Creates MapReduce application components.

    The classes to use for each component must be specified as arguments
    to the constructor.
    """

    @abstractmethod
    def create_mapper(self, context):
        assert isinstance(context, MapContext)

    @abstractmethod
    def create_reducer(self, context):
        assert isinstance(context, ReduceContext)

    def create_combiner(self, context):
        """
        Create a combiner object.

        Return the new combiner or :obj:`None`, if one is not needed.
        """
        assert isinstance(context, MapContext)
        return None

    def create_partitioner(self, context):
        """
        Create a partitioner object.

        Return the new partitioner or :obj:`None`, if the default partitioner
        should be used.
        """
        assert isinstance(context, MapContext)
        return None

    def create_record_reader(self, context):
        """
        Create a record reader object.

        Return the new record reader or :obj:`None`, if the Java record
        reader should be used.
        """
        assert isinstance(context, MapContext)
        return None

    def create_record_writer(self, context):
        """
        Create an application record writer.

        Return the new record writer or :obj:`None`, if the Java record
        writer should be used.
        """
        assert isinstance(context, ReduceContext)
        return None
