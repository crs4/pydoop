import unittest

from test_context import *

import hadoop_pipes

class Mapper(hadoop_pipes.Mapper):
  def __init__(self, task_ctx):
    hadoop_pipes.Mapper.__init__(self)
    print 'Mapper has been instantiated'
  def map(self, map_ctx):
    print 'Mapper.map has been invoked'

class Reducer(hadoop_pipes.Reducer):
  def __init__(self, task_ctx):
    hadoop_pipes.Reducer.__init__(self)
    print 'Reducer has been instantiated'
  def reduce(self, map_ctx):
    print 'Reducer.reduce has been invoked'

class Factory(hadoop_pipes.Factory):
  def __init__(self, mapper_class, reducer_class):
    hadoop_pipes.Factory.__init__(self)
    self.mapper_class  = mapper_class
    self.reducer_class = reducer_class
    self.produced      = []

  def createMapper(self, ctx):
    print 'Factory.createMapper'
    o = self.mapper_class(ctx)
    self.produced.append(o)
    return o

  def createReducer(self, ctx):
    o = self.reducer_class(ctx)
    self.produced.append(o)
    return o

if __name__ == '__main__' :
  fact = Factory(Mapper, Reducer)
  mctx = mc()
  rctx = rc()
  m    = fact.createMapper(mctx)
  m.map(mctx)
  r    = fact.createReducer(rctx)
  r.reduce(rctx)
  test_factory = hadoop_pipes.TestFactory(fact)
  m = test_factory.createMapper(mctx)
  m.map(mctx)
  r = test_factory.createReducer(rctx)
  r.reduce(rctx)
  hadoop_pipes.runTask(fact)











