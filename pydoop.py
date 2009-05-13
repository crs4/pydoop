import hadoop_pipes

from hadoop_pipes import runTask as runTask
from hadoop_pipes import Mapper  as Mapper
from hadoop_pipes import Reducer as Reducer

import sys

def log(x):
  sys.stderr.write('Factory::%s\n' % x)


class Factory(hadoop_pipes.Factory):
  def __init__(self, mapper_class, reducer_class):
    log('start factory init')
    hadoop_pipes.Factory.__init__(self)
    self.mapper_class  = mapper_class
    self.reducer_class = reducer_class
    self.produced      = []
    log('end factory init')

  def __del__(self):
    log('they are killing me!')
    for o in self.produced:
      print o
    log('will all die with me.')

  def createMapper(self, x):
    jc = x.getJobConf()
    o = self.mapper_class(x)
    log('createMapper:: created %s' % o)
    del o
    o = self.mapper_class(x)
    self.produced.append(o)
    return o

  def createReducer(self, ctx):
    o = self.reducer_class(ctx)
    self.produced.append(o)
    return o

