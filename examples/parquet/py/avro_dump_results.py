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

import pydoop
import pydoop.mapreduce.api as api
import pydoop.mapreduce.pipes as pp

from avro.io import DatumReader, BinaryDecoder
import avro


AVRO_INPUT = pydoop.PROPERTIES['AVRO_INPUT']
AVRO_VALUE_INPUT_SCHEMA = pydoop.PROPERTIES['AVRO_VALUE_INPUT_SCHEMA']


def get_schema(jc):
    """
    Get schema from JSON string.
    """
    schema_str = jc.get(AVRO_VALUE_INPUT_SCHEMA)
    return avro.schema.parse(schema_str)


class AvroContext(pp.TaskContext):

    def set_job_conf(self, vals):
        super(AvroContext, self).set_job_conf(vals)
        jc = self.get_job_conf()
        if AVRO_INPUT in jc:
            assert jc.get(AVRO_INPUT).upper() == 'V'
            schema = get_schema(jc)
            self.datum_reader = DatumReader(schema)

    def get_input_value(self):
        # FIXME reuse, reuse, reuse
        sys.stderr.write('value: %r\n' % self._value)
        f = StringIO(self._value)
        dec = BinaryDecoder(f)
        return self.datum_reader.read(dec)


class Mapper(api.Mapper):

    def map(self, ctx):
        cc_stat = ctx.value
        ctx.emit(cc_stat['office'], repr(cc_stat['counts']))


def __main__():
    pp.run_task(
        pp.Factory(mapper_class=Mapper), context_class=AvroContext
    )
