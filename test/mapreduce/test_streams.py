# BEGIN_COPYRIGHT
#
# Copyright 2009-2016 CRS4.
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
from pydoop.mapreduce.streams import get_key_value_stream
from pydoop.mapreduce.streams import get_key_values_stream


STREAM_1_DATA = [
    ('mapItem', 'key1', 'val1'),
    ('mapItem', 'key2', 'val2'),
    ('mapItem', 'key3', 'val3'),
    ('close',),
    ('mapItem', 'key3', 'val3'),  # should not get here
]

STREAM_2_DATA = [
    ('reduceKey', 'key1'),
    ('reduceValue', 'val11'),
    ('reduceValue', 'val12'),
    ('reduceValue', 'val13'),
    ('reduceKey', 'key2'),
    ('reduceValue', 'val21'),
    ('reduceValue', 'val22'),
    ('reduceValue', 'val23'),
    ('close',),
    ('reduceValue', 'val24'),  # should not get here
]


key = None


def keyfunc(t):
    global key
    if t[0] == 'reduceKey':
        key = t[1]
    return key


def get_stream(data):
    for args in data:
        yield (args[0], tuple(args[1:]))


class TestStream(unittest.TestCase):

    def test_get_key_value_stream(self):
        stream = get_stream(STREAM_1_DATA)
        kv_stream = get_key_value_stream(stream)
        for ((k, v), (cmd, k1, v1)) in it.izip(kv_stream, STREAM_1_DATA):
            self.assertEqual(k, k1)
            self.assertEqual(v, v1)

    def test_get_key_values_stream(self):
        stream = get_stream(STREAM_2_DATA)
        kvs_stream = get_key_values_stream(stream, private_encoding=False)
        kvs2_stream = it.groupby(STREAM_2_DATA, keyfunc)

        for (k1, vals1), (k2, itx) in it.izip(kvs_stream, kvs2_stream):
            self.assertEqual(k1, k2)
            vals2 = it.imap(lambda t: t[1],
                            it.ifilter(lambda t: t[0] == 'reduceValue', itx))
            for v1, v2 in it.izip(vals1, vals2):
                self.assertEqual(v1, v2)

    def test_get_key_values_stream2(self):
        stream = get_stream(STREAM_2_DATA)
        kvs_stream = get_key_values_stream(stream, private_encoding=False)
        kvs2_stream = it.groupby(STREAM_2_DATA, keyfunc)
        for (k1, vals1), (k2, itx) in it.izip(kvs_stream, kvs2_stream):
            self.assertEqual(k1, k2)
            if k1 == 'key2':
                vals2 = it.imap(lambda t: t[1],
                                it.ifilter(lambda t: t[0] == 'reduceValue',
                                           itx))
                for v1, v2 in it.izip(vals1, vals2):
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
