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

import sys
from cStringIO import StringIO
from collections import Counter

import pydoop.mapreduce.api as api
import pydoop.mapreduce.pipes as pp

from avro.io import DatumReader, BinaryDecoder
import avro


AVRO_MODE = 'pydoop.mapreduce.pipes.avro.mode'
AVRO_VALUE_SCHEMA = 'pydoop.mapreduce.pipes.avro.value.schema'


def get_schema(jc):
    """
    Get schema from JSON string.
    """
    schema_str = jc.get(AVRO_VALUE_SCHEMA)
    return avro.schema.parse(schema_str)


def get_schema_alt(jc):
    """
    Get schema from parsed JSON (alternate method for doc purposes).
    """
    schema_json = jc.get_json(AVRO_VALUE_SCHEMA)
    return avro.schema.make_avsc_object(schema_json)


class AvroContext(pp.TaskContext):

    datum_reader = None  # FIXME not strictly necessary

    def set_job_conf(self, vals):
        super(AvroContext, self).set_job_conf(vals)
        jc = self.get_job_conf()
        assert jc.get_bool(AVRO_MODE)
        try:
            schema = get_schema(jc)
        # FIXME: AVRO_VALUE_SCHEMA is *not* set in the reducer's context
        except RuntimeError:
            pass
        else:
            assert get_schema_alt(jc).to_json() == schema.to_json()
            self.datum_reader = DatumReader(schema)

    def get_input_value(self):
        # FIXME reuse, reuse, reuse
        sys.stderr.write('value: %r\n' % self._value)
        f = StringIO(self._value)
        dec = BinaryDecoder(f)
        return self.datum_reader.read(dec)


class ColorPick(api.Mapper):

    def __init__(self, ctx):
        super(ColorPick, self).__init__(ctx)
        is_avro_mode = ctx.job_conf.get_bool(AVRO_MODE)
        schema = ctx.job_conf.get_json(AVRO_VALUE_SCHEMA)
        sys.stderr.write('avro mode: %r\n' % (is_avro_mode,))
        sys.stderr.write('avro value schema: %r\n' % (schema,))

    def map(self, ctx):
        user = ctx.value
        color = user['favorite_color']
        sys.stderr.write('user: %r\n' % (user,))
        if color is not None:
            ctx.emit(user['office'], Counter({color: 1}))


class ColorCount(api.Reducer):

    def __init__(self, ctx):
        super(ColorCount, self).__init__(ctx)
        is_avro_mode = ctx.job_conf.get_bool(AVRO_MODE)
        sys.stderr.write('avro mode: %r\n' % (is_avro_mode,))

    def reduce(self, ctx):
        s = sum(ctx.values, Counter())
        ctx.emit(ctx.key, "%r" % s)


def __main__():
    pp.run_task(
        pp.Factory(mapper_class=ColorPick, reducer_class=ColorCount),
        private_encoding=True, context_class=AvroContext
    )
