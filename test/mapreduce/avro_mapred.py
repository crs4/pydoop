#!/usr/bin/env python

import pydoop.mapreduce.api as api
import pydoop.mapreduce.pipes as pp
from pydoop.avrolib import AvroContext


class Mapper(api.Mapper):
    def map(self, context):
        context.emit('', context.value['population'])


class Reducer(api.Reducer):
    def reduce(self, context):
        context.emit('', sum(context.values))


FACTORY = pp.Factory(Mapper, Reducer)
CONTEXT = AvroContext


def __main__():
    pp.run_task(FACTORY, private_encoding=True, context_class=CONTEXT)
