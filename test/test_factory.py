import unittest

from test_context import *

import pydoop_core


class Mapper(pydoop_core.Mapper):
  def __init__(self, task_ctx):
    pydoop_core.Mapper.__init__(self)
    print 'Mapper has been instantiated'
  def map(self, map_ctx):
    print 'Mapper.map has been invoked'
    k = map_ctx.getInputKey()
    v = map_ctx.getInputValue()
    print 'Mapper.map InputKey=%s'   % k
    print 'Mapper.map InputValue=%s' % v
    map_ctx.emit(k, v)

class Reducer(pydoop_core.Reducer):
  def __init__(self, task_ctx):
    pydoop_core.Reducer.__init__(self)
    print 'Reducer has been instantiated'
  def reduce(self, map_ctx):
    print 'Reducer.reduce has been invoked'

class Factory(pydoop_core.Factory):
  def __init__(self, mapper_class, reducer_class):
    pydoop_core.Factory.__init__(self)
    self.mapper_class  = mapper_class
    self.reducer_class = reducer_class
    self.produced      = []

  def createMapper(self, ctx):
    print '--createMapper() Factory.createMapper'
    o = self.mapper_class(ctx)
    print '--createMapper() Factory.createMapper: %r' % o
    self.produced.append(o)
    print '--createMapper() Factory.createMapper: ready to return'
    return o

  def createReducer(self, ctx):
    o = self.reducer_class(ctx)
    self.produced.append(o)
    return o

if __name__ == '__main__' :
  j = jc(jc_fields)
  fact = Factory(Mapper, Reducer)
  mctx = mc(j)
  rctx = rc(j)
  m    = fact.createMapper(mctx)
  m.map(mctx)
  r    = fact.createReducer(rctx)
  r.reduce(rctx)
  test_factory = pydoop_core.TestFactory(fact)
  print test_factory
  m = test_factory.createMapper(mctx)
  m.map(mctx)
  r = test_factory.createReducer(rctx)
  r.reduce(rctx)
  pydoop_core.try_reducer(r, rctx)
  pydoop_core.try_mapper(m, mctx)
  pydoop_core.try_factory(fact, mctx, rctx)
  pydoop_core.try_factory_internal(fact)


  #pydoop_core.runTask(fact)











