#!/usr/bin/env python

# BEGIN_COPYRIGHT
# END_COPYRIGHT

"""
This example includes only the bare minimum required to run
wordcount. See wordcount-full.py for an example that uses counters,
RecordReader, etc.
"""

import sys
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
    context.emit(context.getInputKey(), str(s))


def main(argv):
  runTask(Factory(WordCountMapper, WordCountReducer))


if __name__ == "__main__":
  main(sys.argv)
