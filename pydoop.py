import hadoop_pipes

from hadoop_pipes import runTask as runTask
from hadoop_pipes import Mapper  as Mapper
from hadoop_pipes import Reducer as Reducer

import sys

def log(x):
  sys.stderr.write('%s\n' % x)


class Factory(hadoop_pipes.Factory):
  def __init__(self, mapper_class, reducer_class):
    log('start factory init')
    hadoop_pipes.Factory.__init__(self)
    self.mapper_class  = mapper_class
    self.reducer_class = reducer_class
    self.produced      = []
    log('end factory init')

  def createMapper(self, x):
    log('start createMapper zag -- %s' % x)
    log('dir(ctx) = %s' % dir(x))
    jc = x.getJobConf()
    log('jc[io.sort.mb =%s' % jc.getInt('io.sort.mb'))
    o = self.mapper_class(x)
    self.produced.append(o)
    log('type(o) = %s' % type(o))
    log('dir(o) = %s' % dir(o))
    return o

  def createReducer(self, ctx):
    o = self.reducer_class(ctx)
    self.produced.append(o)
    return o

