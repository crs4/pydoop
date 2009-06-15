import pydoop_core

from pydoop_core import runTask as runTask
from pydoop_core import Mapper  as Mapper
from pydoop_core import Reducer as Reducer
from pydoop_core import RecordReader as RecordReader

import sys


class Factory(pydoop_core.Factory):
  def __init__(self, mapper_class, reducer_class):
    pydoop_core.Factory.__init__(self)
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

