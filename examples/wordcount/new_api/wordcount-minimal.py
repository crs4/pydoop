#!/usr/bin/env python

import itertools as it
from pydoop.mapreduce.pipes import run_task, Factory
from pydoop.mapreduce.api import Mapper, Reducer, Partitioner


class TMapper(Mapper):

    def __init__(self, ctx):
        self.ctx = ctx
        print "Mapper instantiated"

    def map(self, ctx):
        words = ''.join(c for c in ctx.value
                        if c.isalnum() or c == ' ').lower().split()
        for w in words:
            ctx.emit(w, '1')


class TReducer(Reducer):

    def __init__(self, ctx):
        self.ctx = ctx
        print "Reducer instantiated"

    def reduce(self, ctx):
        s = sum(it.imap(int, ctx.values))
        ctx.emit(ctx.key, str(s))


if __name__ == "__main__":
    run_task(Factory(mapper_class=TMapper, reducer_class=TReducer))
