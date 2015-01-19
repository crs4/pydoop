#!/usr/bin/env python

# BEGIN_COPYRIGHT
#
# Copyright 2009-2015 CRS4.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy
# of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.
#
# END_COPYRIGHT

"""
Processing avro-encoded parquet data
====================================
"""
import sys
from cStringIO import StringIO
from collections import Counter

import pydoop.mapreduce.api as api
import pydoop.mapreduce.pipes as pp

from avro.io import DatumReader, BinaryDecoder
import avro


AVRO_SCHEMA_KEY = 'avro.schema'


class AvroContext(pp.TaskContext):

    datum_reader = None  # FIXME not strictly necessary

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
            ctx.emit(user['office'], Counter({color: 1}))


class ColorCount(api.Reducer):

    def reduce(self, ctx):
        s = sum(ctx.values, Counter())
        ctx.emit(ctx.key, "%r" % s)


pp.run_task(
    pp.Factory(mapper_class=ColorPick, reducer_class=ColorCount),
    private_encoding=True, context_class=AvroContext
)
