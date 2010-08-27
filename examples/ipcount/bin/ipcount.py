#!/usr/bin/env python

# BEGIN_COPYRIGHT
# END_COPYRIGHT

import sys
import pydoop.pipes as pp
import pydoop.utils as pu


class Mapper(pp.Mapper):

  def __init__(self, context):
    super(Mapper, self).__init__(context)
    context.setStatus("Initialization started")
    self.excluded_counter = context.getCounter("IPCOUNT", "EXCLUDED_LINES")
    jc = context.getJobConf()
    pu.jc_configure(self, jc, "ipcount.excludes", "excludes_fn", "")
    if self.excludes_fn:
      f = open(self.excludes_fn)
      self.excludes = set([line.strip() for line in f])
      f.close()
    else:
      self.excludes = set([])
    context.setStatus("Initialization done")

  def map(self, context):
    ip = context.getInputValue().split(None,1)[0]
    if ip not in self.excludes:
      context.emit(ip, "1")
    else:
      context.incrementCounter(self.excluded_counter, 1)


class Reducer(pp.Reducer):

  def reduce(self, context):
    s = 0
    while context.nextValue():
      s += int(context.getInputValue())
    context.emit(context.getInputKey(), str(s))


def main(argv):
  pp.runTask(pp.Factory(Mapper, Reducer, combiner_class=Reducer))


if __name__ == "__main__":
  main(sys.argv)
