# BEGIN_COPYRIGHT
#
# Copyright 2009-2018 CRS4.
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

import pydoop.mapreduce.api as api
import pydoop.mapreduce.pipes as pp
import pydoop.avrolib as avrolib
from pydoop.test_utils import WDTestCase
from pydoop.mapreduce.binary_streams import (
    BinaryWriter, BinaryDownStreamAdapter
)
from pydoop.utils.py3compat import iteritems

from common import AvroSerializer, avro_user_record
from pydoop.config import (
    AVRO_INPUT, AVRO_KEY_INPUT_SCHEMA, AVRO_VALUE_INPUT_SCHEMA
)

THIS_DIR = os.path.dirname(os.path.abspath(__file__))


class BaseMapper(api.Mapper):

    def map(self, ctx):
        k, v = self.get_kv(ctx)
        ctx.emit(k, v)

    def get_kv(self, ctx):
        raise NotImplementedError


class ValueMapper(BaseMapper):

    def get_kv(self, ctx):
        return ctx.value['name'], ctx.value['favorite_color']


class KeyMapper(BaseMapper):

    def get_kv(self, ctx):
        return ctx.key['name'], ctx.key['favorite_color']


class DictWrapper(object):

    def __init__(self, d):
        self.__d = d

    def __getattr__(self, name):
        try:
            return self.__d[name]
        except KeyError:
            raise AttributeError("no attribute %s" % name)


class WrapperAvroContext(avrolib.AvroContext):

    def deserializing(self, meth, datum_reader):
        def deserialize_and_wrap(*args, **kwargs):
            deserialize = super(WrapperAvroContext, self).deserializing(
                meth, datum_reader
            )
            return DictWrapper(deserialize(*args, **kwargs))
        return deserialize_and_wrap


class KeyWrapperMapper(BaseMapper):

    def get_kv(self, ctx):
        return ctx.key.name, ctx.key.favorite_color


class ValueWrapperMapper(BaseMapper):

    def get_kv(self, ctx):
        return ctx.value.name, ctx.value.favorite_color


class TestContext(WDTestCase):

    def setUp(self):
        super(TestContext, self).setUp()
        with open(os.path.join(THIS_DIR, "user.avsc")) as f:
            self.schema = avrolib.parse(f.read())
        self.records = [avro_user_record(_) for _ in range(3)]

    def __write_cmd_file(self, mode):
        if mode != 'K' and mode != 'V':
            # FIXME: add support for 'KV'
            raise RuntimeError("Mode %r not supported" % (mode,))
        schema_prop = (AVRO_KEY_INPUT_SCHEMA if mode == 'K'
                       else AVRO_VALUE_INPUT_SCHEMA)
        cmd_fn = self._mkfn('map_in')
        serializer = AvroSerializer(self.schema)
        with open(cmd_fn, 'wb') as f:
            bw = BinaryWriter(f)
            bw.send(bw.START_MESSAGE, 0)
            bw.send(bw.SET_JOB_CONF,
                    AVRO_INPUT, mode,
                    schema_prop, str(self.schema),
                    'mapreduce.pipes.isjavarecordreader', 'true',
                    'mapreduce.pipes.isjavarecordwriter', 'true')
            bw.send(bw.RUN_MAP, 'input_split', 0, True)
            bw.send(bw.SET_INPUT_TYPES, 'key_type', 'value_type')
            for r in self.records:
                if mode == 'K':
                    bw.send(bw.MAP_ITEM, serializer.serialize(r), 'v')
                else:
                    bw.send(bw.MAP_ITEM, 'k', serializer.serialize(r))
            bw.send(bw.CLOSE)
            bw.close()
        return cmd_fn

    def tearDown(self):
        super(TestContext, self).tearDown()

    def __run_test(self, mode, mapper_class, context_class):
        cmd_file = self.__write_cmd_file(mode)
        pp.run_task(
            pp.Factory(mapper_class=mapper_class), private_encoding=False,
            context_class=context_class, cmd_file=cmd_file)
        out_fn = cmd_file + '.out'
        out_records = []
        with open(out_fn, 'rb') as f:
            bf = BinaryDownStreamAdapter(f)
            for cmd, args in bf:
                if cmd == bf.OUTPUT:
                    name, color = args
                    out_records.append({'name': name, 'favorite_color': color})
        self.assertEqual(len(out_records), len(self.records))
        for out_r, r in zip(out_records, self.records):
            for k, v in iteritems(out_r):
                self.assertEqual(v.decode('UTF-8'), r[k])

    def test_key(self):
        self.__run_test('K', KeyMapper, avrolib.AvroContext)

    def test_value(self):
        self.__run_test('V', ValueMapper, avrolib.AvroContext)

    def test_wrapper_key(self):
        self.__run_test('K', KeyWrapperMapper, WrapperAvroContext)

    def test_wrapper_value(self):
        self.__run_test('V', ValueWrapperMapper, WrapperAvroContext)


def suite():
    suite_ = unittest.TestSuite()
    suite_.addTest(TestContext('test_key'))
    suite_.addTest(TestContext('test_value'))
    suite_.addTest(TestContext('test_wrapper_key'))
    suite_.addTest(TestContext('test_wrapper_value'))
    return suite_


if __name__ == '__main__':
    _RUNNER = unittest.TextTestRunner(verbosity=2)
    _RUNNER.run((suite()))
