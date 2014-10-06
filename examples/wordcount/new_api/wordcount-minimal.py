#!/usr/bin/env python

import itertools as it
import re

from pydoop.mapreduce.pipes import run_task, Factory
from pydoop.mapreduce.api import Mapper, Reducer, Partitioner


class TMapper(Mapper):

    def __init__(self, ctx):
        self.ctx = ctx
        print "Mapper instantiated"

    def map(self, ctx):
        words = re.sub('[^0-9a-zA-Z]+', ' ', ctx.value).split()
        for w in words:
            ctx.emit(w, 1)


class TReducer(Reducer):

    def __init__(self, ctx):
        self.ctx = ctx
        print "Reducer instantiated"

    def reduce(self, ctx):
        s = sum(ctx.values)
        # Note: we explicitly write the value as a str.
        ctx.emit(ctx.key, str(s)) 


if __name__ == "__main__":
    run_task(Factory(mapper_class=TMapper, reducer_class=TReducer), 
             private_encoding=True)
