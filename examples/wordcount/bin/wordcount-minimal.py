#!/usr/bin/env python

"""
This example includes only the bare minimum required to run
wordcount. See wordcount-full.py for an example that uses counters,
RecordReader, etc.
"""


import pydoop.pipes as pp


class Mapper(pp.Mapper):

    def __init__(self, context):
        print context

    def map(self, context):
        words = context.getInputValue().split()
        for w in words:
            context.emit(w, "1")
        print "Mapper: key=%s" % context.getInputKey()


class Reducer(pp.Reducer):

    def __init__(self, context):
        print "Map"

    def reduce(self, context):
        s = 0
        while context.nextValue():
            s += int(context.getInputValue())
        context.emit(context.getInputKey(), str(s))


if __name__ == "__main__":
    pp.runTask(pp.Factory(mapper_class=Mapper, reducer_class=Reducer))
