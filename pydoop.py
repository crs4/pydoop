import hadoop_pipes

from hadoop_pipes import runTask as runTask
from hadoop_pipes import Mapper  as Mapper
from hadoop_pipes import Reducer as Reducer

import sys


class Factory(hadoop_pipes.Factory):
  def __init__(self, mapper_class, reducer_class):
    hadoop_pipes.Factory.__init__(self)
    self.mapper_class  = mapper_class
    self.reducer_class = reducer_class
    self.produced      = []

  def __del__(self):
    # sys.stderr.write('Destroying factory\n')
    pass

  def createMapper(self, x):
    o = self.mapper_class(x)
    self.produced.append(o)
    return o

  def createReducer(self, ctx):
    o = self.reducer_class(ctx)
    self.produced.append(o)
    return o
