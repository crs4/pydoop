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

import unittest
import itertools as it

import pydoop.mapreduce.streams as streams
from pydoop.mapreduce.streams import get_key_value_stream
from pydoop.mapreduce.streams import get_key_values_stream
from pydoop.utils.py3compat import czip, cmap, cfilter

from data.stream_data import STREAM_1_DATA, STREAM_2_DATA

key = None


def keyfunc(t):
    global key
    if t[0] == streams.REDUCE_KEY:
        key = t[1]
    return key


def get_stream(data):
    for args in data:
        yield (args[0], tuple(args[1:]))


class TestStream(unittest.TestCase):

    def test_get_key_value_stream(self):
        stream = get_stream(STREAM_1_DATA)
        kv_stream = get_key_value_stream(stream)
        for ((k, v), (cmd, k1, v1)) in czip(kv_stream, STREAM_1_DATA):
            self.assertEqual(k, k1)
            self.assertEqual(v, v1)

    def test_get_key_values_stream(self):
        stream = get_stream(STREAM_2_DATA)
        kvs_stream = get_key_values_stream(stream, private_encoding=False)
        kvs2_stream = it.groupby(STREAM_2_DATA, keyfunc)

        for (k1, vals1), (k2, itx) in czip(kvs_stream, kvs2_stream):
            self.assertEqual(k1, k2)
            vals2 = cmap(lambda t: t[1],
                         cfilter(lambda t: t[0] == streams.REDUCE_VALUE,
                                 itx))
            for v1, v2 in czip(vals1, vals2):
                self.assertEqual(v1, v2)

    def test_get_key_values_stream2(self):
        stream = get_stream(STREAM_2_DATA)
        kvs_stream = get_key_values_stream(stream, private_encoding=False)
        kvs2_stream = it.groupby(STREAM_2_DATA, keyfunc)
        for (k1, vals1), (k2, itx) in czip(kvs_stream, kvs2_stream):
            self.assertEqual(k1, k2)
            if k1 == 'key2':
                vals2 = cmap(lambda t: t[1],
                             cfilter(lambda t: t[0] == streams.REDUCE_VALUE,
                                     itx))
                for v1, v2 in czip(vals1, vals2):
                    self.assertEqual(v1, v2)


def suite():
    suite_ = unittest.TestSuite()
    suite_.addTest(TestStream('test_get_key_value_stream'))
    suite_.addTest(TestStream('test_get_key_values_stream'))
    suite_.addTest(TestStream('test_get_key_values_stream2'))
    return suite_


if __name__ == '__main__':
    _RUNNER = unittest.TextTestRunner(verbosity=2)
    _RUNNER.run((suite()))
