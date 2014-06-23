from abc import ABCMeta, abstractmethod

"""
Pydoop API
==========

All computation is performed by instances of classes derived from Mapper,
Reducer and, possibly, Reader, Writer, Combiner and Partitioner.

All communication between the framework and these objects is mediated by
Context objects. That is:

 * when they are instantiated, they receive contextual
   information via a context object,::

     def __init__(self, context):
         job_conf = context.job_conf
         input_split = context.input_split

 * when they are used, for instance invoking the map method of a mapper,
   they read data and emit back to the framework via a context object.::

     def map(self, context):
         key, value = context.key, context.value
         ...
         context.emit(new_key, new_value)

"""



class PydoopError(Exception):
    pass

class Counter(object):
    "An interface to Hadoop counters infrastructure."
    def __init__(self, counter_id):
        self.id = counter_id
    def get_id(self):
        return self.id

class JobConf(dict):
    """Configuration properties assigned to this job."""
    def __init__(self, values):
        if not 1 & len(values):
            raise PydoopError('JobConf.__init__: len(values) should be even')
        super(JobConf, self).__init__(zip(values[::2], values[1::2]))
    
    def get_int(self, key, default=None):
        return int(self.get(key, default))
    def get_float(self, key, default=None):
        return float(self.get(key, default))
    def get_bool(self, key, default=None):
        return bool(self.get(key, default))

class Context(object):
    """Information inter-exchange object.

       Context instances are the only points of contact between the framework
       and user defined code.
    """    
    __metaclass__ = ABCMeta

    @abstractmethod
    def get_job_conf(self):
        pass
    @abstractmethod
    def get_input_key(self):
        pass
    @abstractmethod
    def get_input_value(self):
        pass
    @abstractmethod
    def emit(self, key, value):
        pass
    @abstractmethod
    def progress(self):
        pass
    @abstractmethod
    def set_status(self, status):
        pass
    @abstractmethod
    def get_counter(self, group, name):
        pass
    @abstractmethod
    def increment_counter(self, counter, amount):
        pass
    
class MapContext(Context):
    @abstractmethod
    def get_input_split(self):
        pass
    @abstractmethod
    def get_input_key_class(self):
        pass
    @abstractmethod
    def get_input_value_class(self):
        pass

class ReduceContext(Context):
    @abstractmethod
    def next_value(self):
        pass

class Closable(object):
    def close(self):
        pass

class Mapper(Closable):
    __metaclass__ = ABCMeta

    @abstractmethod
    def map(self, context):
        assert isinstance(context, MapContext)


class Reducer(Closable):
    __metaclass__ = ABCMeta

    @abstractmethod
    def reduce(self, context):
        assert isinstance(context, ReduceContext)


class Partitioner(object):
    __metaclass__ = ABCMeta

    @abstractmethod    
    def partition(self, key, num_of_reduces):
        assert isinstance(key, str)
        assert isinstance(num_of_reduces, int)        

class RecordReader(Closable):
    __metaclass__ = ABCMeta

    @abstractmethod    
    def next(self):
        """returns the tuple (key, value). Raises a StopIteration exception
           when the data stream has been emptied.
           """
        raise StopIteration

    @abstractmethod
    def get_progress(self):
        """The progress of the record reader through the split
           as a value between  0.0 and 1.0."""
        pass
        
class RecordWriter(Closable):
    __metaclass__ = ABCMeta

    @abstractmethod
    def emit(self, key, value):
        pass

class Factory(object):
    __metaclass__ = ABCMeta

    @abstractmethod    
    def create_mapper(self, context):
        assert isinstance(context, MapContext)

    @abstractmethod
    def create_reducer(self, context):
        assert isinstance(context, ReduceContext)

    def create_combiner(self, context):
        """Create a combiner, if this application has one.
           Returns the new combiner or None, if one is not needed.
        """        
        assert isinstance(context, MapContext)
        return None

    def create_partitioner(self, context):
        """Create an application partitioner object.
        return the new partitioner or None,
        if the default partitioner should be used."""
        assert isinstance(context, MapContext)
        return None

    def create_record_reader(self, context):
        """Create an application record reader.
        returns the new RecordReader or None, if the Java RecordReader should be
        used."""
        assert isinstance(context, MapContext)        
        return None

    def create_record_writer(self, context):
        """Create an application record writer.
           return the new RecordWriter or None, if the
           Java RecordWriter should be used."""
        assert isinstance(context, ReduceContext)
        return None

