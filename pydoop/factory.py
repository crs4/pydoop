import pydoop_pipes

import sys

class Factory_disabled(pydoop_pipes.Factory):
  def __init__(self, mapper_class, reducer_class):
    pydoop_pipes.Factory.__init__(self)
    #--
    self.mapper_class  = mapper_class
    self.reducer_class  = reducer_class
    self.produced      = []

  def __del__(self):
    # sys.stderr.write('Destroying factory\n')
    pass

  #--
  def createMapper(self, ctx):
    o = self.mapper_class(ctx)
    self.produced.append(o)
    return o
  #--
  def createReducer(self, ctx):
    o = self.reducer_class(ctx)
    self.produced.append(o)
    return o


class Factory(pydoop_pipes.Factory):
  def __init__(self, mapper_class, reducer_class,
               record_reader_class=None,
               record_writer_class=None,
               combiner_class=None,
               partitioner_class=None,
               ):
    pydoop_pipes.Factory.__init__(self)
    #--
    self.mapper_class  = mapper_class
    setattr(self, 'createMapper', self.__make_creator('mapper_class'))
    #--
    self.reducer_class  = reducer_class
    setattr(self, 'createReducer', self.__make_creator('reducer_class'))
    #--
    if record_reader_class is not None:
      self.record_reader_class = record_reader_class
      setattr(self, 'createRecordReader',
              self.__make_creator('record_reader_class'))
    #--
    if record_writer_class is not None:
      self.record_writer_class = record_writer_class
      setattr(self, 'createRecordWriter',
              self.__make_creator('record_writer_class'))
    #--
    if combiner_class is not None:
      self.combiner_class = combiner_class
      setattr(self, 'createRecordWriter',
              self.__make_creator('combiner_class'))
    #--
    if partitioner_class is not None:
      self.partitioner_class = partitioner_class
      setattr(self, 'createRecordWriter',
              self.__make_creator('partitioner_class'))
    #--
    self.produced      = []

  def __del__(self):
    # sys.stderr.write('Destroying factory\n')
    pass

  #--
  def __make_creator(self, name):
    cls = getattr(self, name)
    def __make_creator_helper(ctx):
      o = cls(ctx)
      self.produced.append(o)
      return o
    return __make_creator_helper

