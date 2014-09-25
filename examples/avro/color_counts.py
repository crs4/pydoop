#!/usr/bin/env python
"""
Processing avro encoded data
============================


"""

import pydoop.mapreduce.api as api
import pydoop.mapreduce.pipes as pp

from pydoop.avrolib import AvroReader, AvroWriter
import avro.schema

from collections import Counter
import cPickle
import itertools as it

def serialize(x):
    return cPickle.dumps(x, cPickle.HIGHEST_PROTOCOL)

def deserialize(x):
    return cPickle.loads(x)

class UserReader(AvroReader):
    pass

class ColorWriter(AvroWriter):
    schema = avro.schema.parse(open("stats.avsc").read())
    def emit(self, key, value):
        self.writer.append({'office': key, 'counts': deserialize(value)})
        
class ColorPick(api.Mapper):
    def map(self, ctx):
        user = ctx.value
        color = user['favorite_color']
        if color is not None:
            ctx.emit(user['office'], serialize(Counter({color : 1})))

class ColorCount(api.Reducer):
    def reduce(self, ctx):
        s = sum(it.imap(deserialize, ctx.values), Counter())
        ctx.emit(ctx.key, serialize(s))

pp.run_task(pp.Factory(mapper_class=ColorPick,
                       reducer_class=ColorCount,
                       record_reader_class=UserReader,
                       record_writer_class=ColorWriter))
