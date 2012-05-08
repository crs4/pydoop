#!/usr/bin/env python

# BEGIN_COPYRIGHT
# END_COPYRIGHT

"""
Filter out words whose occurrence falls below a specified value.
"""

import struct
from pydoop.pipes import Mapper, Reducer, Factory, runTask
from pydoop.utils import jc_configure_int


class FilterMapper(Mapper):
  """
  Process a wordcount output stream, emitting only records relative to
  words whose count is equal to or above the configured threshold.
  """
  def __init__(self, context):
    super(FilterMapper, self).__init__(context)
    jc = context.getJobConf()
    jc_configure_int(self, jc, "filter.occurrence.threshold", "threshold")

  def map(self, context):
    word, occurrence = (context.getInputKey(), context.getInputValue())
    occurrence = struct.unpack(">i", occurrence)[0]
    if occurrence >= self.threshold:
      context.emit(word, str(occurrence))


class FilterReducer(Reducer):

  def reduce(self, context):
    pass


if __name__ == "__main__":
  runTask(Factory(FilterMapper, FilterReducer))
