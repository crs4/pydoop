#!/usr/bin/env python
# BEGIN_COPYRIGHT
# END_COPYRIGHT

import sys, struct
from pydoop.pipes import Mapper, Reducer, Factory, runTask


class WordCountMapper(Mapper):

  def map(self, context):
    words = context.getInputValue().split()
    for w in words:
      context.emit(w, "1")


class WordCountReducer(Reducer):

  def reduce(self, context):
    s = 0
    while context.nextValue():
      s += int(context.getInputValue())
    context.emit(context.getInputKey(), struct.pack(">i", s))


def main(argv):
  runTask(Factory(WordCountMapper, WordCountReducer))


if __name__ == "__main__":
  main(sys.argv)
