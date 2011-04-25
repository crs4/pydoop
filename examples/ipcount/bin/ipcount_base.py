#!/usr/bin/env python

# BEGIN_COPYRIGHT
# END_COPYRIGHT

import pydoop.pipes as pp


class Mapper(pp.Mapper):

  def map(self, context):
    context.emit(context.getInputValue().split(None,1)[0], "1")


class Reducer(pp.Reducer):

  def reduce(self, context):
    s = 0
    while context.nextValue():
      s += int(context.getInputValue())
    context.emit(context.getInputKey(), str(s))


if __name__ == "__main__":
  pp.runTask(pp.Factory(Mapper, Reducer, combiner_class=Reducer))
