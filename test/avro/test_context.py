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


class BaseMapper(api.Mapper):

    def map(self, ctx):
        r = self.get_record(ctx)
        k, v = r['name'], r['favorite_color']
        print ' * k = %r, v = %r' % (k, v)
        ctx.emit(k, v)

    def get_record(self, ctx):
        raise NotImplementedError


class ValueMapper(BaseMapper):

    def get_record(self, ctx):
        return ctx.value


class KeyMapper(BaseMapper):

    def get_record(self, ctx):
        return ctx.key


class TestContext(WDTestCase):

    def setUp(self):
        super(TestContext, self).setUp()
        with open(os.path.join(THIS_DIR, "user.avsc")) as f:
            self.schema = avro.schema.parse(f.read())
        self.records = [avro_user_record(_) for _ in xrange(3)]

    def __write_cmd_file(self, mode):
        if mode != 'K' and mode != 'V':
            # FIXME: add support for 'KV'
            raise RuntimeError("Mode %r not supported" % (mode,))
        schema_prop = pydoop.PROPERTIES[
            'AVRO_%s_INPUT_SCHEMA' % ('KEY' if mode == 'K' else 'VALUE')
        ]
        cmd_fn = self._mkfn('map_in')
        serializer = AvroSerializer(self.schema)
        with open(cmd_fn, 'w') as f:
            bwriter = BinaryWriter(f)
            bwriter.send('start', 0)
            bwriter.send('setJobConf', (
                pydoop.PROPERTIES['AVRO_INPUT'], mode,
                schema_prop, str(self.schema)
            )),
            bwriter.send('setInputTypes', 'key_type', 'value_type')
            bwriter.send('runMap', 'input_split', 0, False)
            for r in self.records:
                if mode == 'K':
                    bwriter.send('mapItem', serializer.serialize(r), 'v')
                else:
                    bwriter.send('mapItem', 'k', serializer.serialize(r))
            bwriter.send('close')
        return cmd_fn

    def tearDown(self):
        super(TestContext, self).tearDown()

    def __run_test(self, mode, mapper_class):
        print
        cmd_file = self.__write_cmd_file(mode)
        pp.run_task(
            pp.Factory(mapper_class=mapper_class), private_encoding=False,
            context_class=avrolib.AvroContext, cmd_file=cmd_file
        )
        out_fn = cmd_file + '.out'
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

    def test_key(self):
        self.__run_test('K', KeyMapper)

    def test_value(self):
        self.__run_test('V', ValueMapper)


def suite():
    suite_ = unittest.TestSuite()
    suite_.addTest(TestContext('test_key'))
    suite_.addTest(TestContext('test_value'))
    return suite_


if __name__ == '__main__':
    _RUNNER = unittest.TextTestRunner(verbosity=2)
    _RUNNER.run((suite()))
