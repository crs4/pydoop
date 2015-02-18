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

from cStringIO import StringIO
from collections import Counter

import pydoop
import pydoop.mapreduce.api as api
import pydoop.mapreduce.pipes as pp

from avro.io import DatumReader, DatumWriter, BinaryDecoder, BinaryEncoder
import avro


AVRO_INPUT = pydoop.PROPERTIES['AVRO_INPUT']
AVRO_OUTPUT = pydoop.PROPERTIES['AVRO_OUTPUT']
AVRO_VALUE_INPUT_SCHEMA = pydoop.PROPERTIES['AVRO_VALUE_INPUT_SCHEMA']
AVRO_VALUE_OUTPUT_SCHEMA = pydoop.PROPERTIES['AVRO_VALUE_OUTPUT_SCHEMA']


def get_schema(jc, schema_prop):
    """
    Get schema from JSON string.
    """
    schema_str = jc.get(schema_prop)
    return avro.schema.parse(schema_str)


def get_schema_alt(jc, schema_prop):
    """
    Get schema from parsed JSON (alternate method for doc purposes).
    """
    schema_json = jc.get_json(schema_prop)
    return avro.schema.make_avsc_object(schema_json)


class AvroContext(pp.TaskContext):

    datum_reader = None  # FIXME not strictly necessary

    def set_job_conf(self, vals):
        super(AvroContext, self).set_job_conf(vals)
        jc = self.get_job_conf()
        # This method is called both in map and reduce tasks.  Since
        # AVRO_INPUT and AVRO_VALUE_SCHEMA are set by PydoopAvroBridgeReader,
        # however, they will only be present in the map task's conf.
        if AVRO_INPUT in jc:
            assert jc.get(AVRO_INPUT).upper() == 'V'
            schema = get_schema(jc, AVRO_VALUE_INPUT_SCHEMA)
            assert get_schema_alt(
                jc, AVRO_VALUE_INPUT_SCHEMA
            ).to_json() == schema.to_json()
            self.datum_reader = DatumReader(schema)
        if AVRO_OUTPUT in jc:
            assert jc.get(AVRO_OUTPUT).upper() == 'V'
            schema = get_schema(jc, AVRO_VALUE_OUTPUT_SCHEMA)
            self.datum_writer = DatumWriter(schema)

    def get_input_value(self):
        f = StringIO(self._value)
        dec = BinaryDecoder(f)
        return self.datum_reader.read(dec)

    def emit(self, key, value):
        if self.__emit_avro():
            f = StringIO()
            encoder = BinaryEncoder(f)
            self.datum_writer.write(value, encoder)
            v = f.getvalue()
        else:
            v = value
        super(AvroContext, self).emit(key, v)

    def __emit_avro(self):
        jc = self.job_conf
        if AVRO_OUTPUT not in jc:
            return False
        assert jc.get(AVRO_OUTPUT).upper() == 'V'
        if self.is_reducer():
            return True
        return jc.get_int(
            'mapred.reduce.tasks', jc.get_int('mapreduce.job.reduces', 0)
        ) < 1


class ColorPick(api.Mapper):

    def map(self, ctx):
        user = ctx.value
        color = user['favorite_color']
        if color is not None:
            ctx.emit(user['office'], Counter({color: 1}))


class ColorCount(api.Reducer):

    def reduce(self, ctx):
        s = sum(ctx.values, Counter())
        ctx.emit(ctx.key, {'office': ctx.key, 'counts': s})


def __main__():
    pp.run_task(
        pp.Factory(mapper_class=ColorPick, reducer_class=ColorCount),
        private_encoding=True, context_class=AvroContext
    )
