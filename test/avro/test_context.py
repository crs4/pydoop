# BEGIN_COPYRIGHT
#
# Copyright 2009-2015 CRS4.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy
# of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.
#
# END_COPYRIGHT

import os
import unittest

import avro.schema

import pydoop
import pydoop.mapreduce.api as api
import pydoop.mapreduce.pipes as pp
import pydoop.avrolib as avrolib
from pydoop.test_utils import WDTestCase
from pydoop.mapreduce.binary_streams import (
    BinaryWriter, BinaryDownStreamFilter
)

from common import AvroSerializer, avro_user_record


THIS_DIR = os.path.dirname(os.path.abspath(__file__))


class Mapper(api.Mapper):

    def map(self, ctx):
        r = ctx.value
        k, v = r['name'], r['favorite_color']
        print ' * k = %r, v = %r' % (k, v)
        ctx.emit(k, v)


class TestContext(WDTestCase):

    def setUp(self):
        super(TestContext, self).setUp()
        with open(os.path.join(THIS_DIR, "user.avsc")) as f:
            schema = avro.schema.parse(f.read())
        self.records = [avro_user_record(_) for _ in xrange(3)]
        serializer = AvroSerializer(schema)
        #--
        self.cmd_fn = self._mkfn('map_in')
        with open(self.cmd_fn, 'w') as f:
            bwriter = BinaryWriter(f)
            bwriter.send('start', 0)
            bwriter.send('setJobConf', (
                pydoop.PROPERTIES['AVRO_INPUT'], 'V',
                pydoop.PROPERTIES['AVRO_VALUE_INPUT_SCHEMA'], str(schema)
            )),
            bwriter.send('setInputTypes', 'key_type', 'value_type')
            bwriter.send('runMap', 'input_split', 0, False)
            for r in self.records:
                bwriter.send('mapItem', 'k', serializer.serialize(r))
            bwriter.send('close')

    def tearDown(self):
        super(TestContext, self).tearDown()

    def runTest(self):
        print
        pp.run_task(
            pp.Factory(mapper_class=Mapper), private_encoding=False,
            context_class=avrolib.AvroContext, cmd_file=self.cmd_fn
        )
        out_fn = self.cmd_fn + '.out'
        out_records = []
        with open(out_fn) as ostream:
            for cmd, args in BinaryDownStreamFilter(ostream):
                if cmd == 'output':
                    name, color = args
                    out_records.append({'name': name, 'favorite_color': color})
        self.assertEqual(len(out_records), len(self.records))
        for out_r, r in zip(out_records, self.records):
            for k, v in out_r.iteritems():
                self.assertEqual(v, r[k])


def suite():
    suite_ = unittest.TestSuite()
    suite_.addTest(TestContext('runTest'))
    return suite_


if __name__ == '__main__':
    _RUNNER = unittest.TextTestRunner(verbosity=2)
    _RUNNER.run((suite()))
