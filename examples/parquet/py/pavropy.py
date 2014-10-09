#!/usr/bin/env python
"""
Processing avro encoded parquet data
====================================


"""

import pydoop.mapreduce.api as api
import pydoop.mapreduce.pipes as pp

from avro.io import DatumReader, BinaryDecoder
import avro
from cStringIO import StringIO


from collections import Counter

import itertools as it

import sys

AVRO_SCHEMA_KEY = 'avro.schema'

class AvroContext(pp.TaskContext):
    datum_reader = None # FIXME not strictly necessary....
    def set_job_conf(self, vals):
        super(AvroContext, self).set_job_conf(vals)
        schema = avro.schema.parse(self._job_conf[AVRO_SCHEMA_KEY])
        self.datum_reader = DatumReader(schema)

    def get_input_value(self):
        # FIXME reuse, reuse, reuse
        sys.stderr.write('value: %r\n' % self._value)
        f = StringIO(self._value)
        dec = BinaryDecoder(f)
        return self.datum_reader.read(dec)

class ColorPick(api.Mapper):
    def map(self, ctx):
        user = ctx.value
        color = user['favorite_color']
        sys.stderr.write('user: %r' % user)
        if color is not None:
            ctx.emit(user['office'], Counter({color : 1}))

class ColorCount(api.Reducer):
    def reduce(self, ctx):
        s = sum(ctx.values, Counter())
        ctx.emit(ctx.key, "%r" % s)

pp.run_task(pp.Factory(mapper_class=ColorPick,
                       reducer_class=ColorCount),
            private_encoding=True,
            context_class=AvroContext
)
