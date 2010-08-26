#!/usr/bin/env python

# BEGIN_COPYRIGHT
# END_COPYRIGHT

"""
Count occurrences of IP addresses in a given Apache access log file.

http://httpd.apache.org/docs/1.3/logs.html#common
"""

import sys
import pydoop.pipes as pp #import Mapper, Reducer, Factory, runTask
from pydoop.utils import jc_configure_int


class Mapper(pp.Mapper):

  def map(self, context):
    context.emit(context.getInputValue().split(None,1)[0], "1")


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
