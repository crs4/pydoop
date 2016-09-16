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
from pydoop.utils.py3compat import czip, unicode

import pydoop.mapreduce.streams as streams
from pydoop.mapreduce.streams import ProtocolError
from pydoop.mapreduce.text_streams import (TextWriter,
                                           TextDownStreamAdapter,
                                           TextUpStreamAdapter)
from pydoop.mapreduce.binary_streams import (BinaryWriter,
                                             BinaryDownStreamAdapter,
                                             BinaryUpStreamAdapter)

from pydoop.test_utils import WDTestCase

STREAM_1 = [
    (streams.START_MESSAGE, 0),
    (streams.SET_JOB_CONF, 'key1', 'value1', 'key2', 'value2'),
    (streams.SET_INPUT_TYPES, 'key_type', 'value_type'),
    (streams.RUN_MAP, 'input_split', 3, 0),
    (streams.MAP_ITEM, 'key1', 'val1'),
    (streams.MAP_ITEM, 'key1', 'val2'),
    (streams.MAP_ITEM, 'key2', 'val3'),
    (streams.RUN_REDUCE, 0, 0),
    (streams.REDUCE_KEY, 'key1'),
    (streams.REDUCE_VALUE, 'val1'),
    (streams.REDUCE_VALUE, 'val2'),
    (streams.REDUCE_KEY, 'key2'),
    (streams.REDUCE_VALUE, 'val3'),
    (streams.CLOSE,),
]

STREAM_2 = [
    (streams.OUTPUT, 'key1', 'val1'),
    (streams.PARTITIONED_OUTPUT, 22, 'key2', 'val2'),
    (streams.STATUS, 'jolly good'),
    (streams.PROGRESS, 0.99),
    (streams.DONE,),
    (streams.REGISTER_COUNTER, 22, 'cgroup', 'cname'),
    (streams.INCREMENT_COUNTER, 22, 123),
]


def stream_writer(fname, data, mod, Writer):
    with open(fname, 'w' + mod) as f:
        writer = Writer(f)
        for vals in data:
            writer.send(vals[0], *vals[1:])
        writer.flush()


class TestCmdStreams(WDTestCase):

    def downlink_helper(self, mod, Writer, DownStreamAdapter):
        fname = "foo.bin" # self._mkfn('foo.' + ('bin' if mod == 'b' else ''))
        stream_writer(fname, STREAM_1, mod, Writer)
        with open(fname, 'r' + mod) as f:
            stream = DownStreamAdapter(f)
            try:
                for (cmd, args), vals in czip(stream, STREAM_1):
                    self.assertEqual(cmd, vals[0])
                    vals = vals[1:]                    
                    if mod == 'b':
                        vals = [x.encode('utf-8')
                                if isinstance(x, unicode) else x
                                for x in vals ]
                    vals = tuple(vals)
                    if cmd == streams.SET_JOB_CONF:
                        self.assertEqual(len(args), 1)
                        self.assertEqual(args[0], vals)
                    else:
                        self.assertTrue((len(vals) == 0 and not args) or
                                        (vals == args))
            except ProtocolError as e:
                print('error -- %s' % e)

    def test_text_downlink(self):
        self.downlink_helper('', TextWriter, TextDownStreamAdapter)

    def test_binary_downlink(self):
        self.downlink_helper('b', BinaryWriter, BinaryDownStreamAdapter)

    def uplink_helper(self, mod, UpStreamAdapter, DownStreamAdapter):
        fname = self._mkfn('foo.txt')
        with open(fname, 'w' + mod) as f:
            stream = UpStreamAdapter(f)
            try:
                for vals in STREAM_2:
                    stream.send(vals[0], *vals[1:])
            except ProtocolError as e:
                print('error -- %s' % e)
        with open(fname, 'r' + mod) as f:
            stream = DownStreamAdapter(f)
            for cmd, vals in stream:
                print(cmd, vals)

    def test_text_uplink(self):
        self.uplink_helper('', TextUpStreamAdapter, TextDownStreamAdapter)

    def test_binary_uplink(self):
        self.uplink_helper('b', BinaryUpStreamAdapter, BinaryDownStreamAdapter)


def suite():
    suite_ = unittest.TestSuite()
    suite_.addTest(TestCmdStreams('test_text_downlink'))
    suite_.addTest(TestCmdStreams('test_binary_downlink'))
    suite_.addTest(TestCmdStreams('test_text_uplink'))
    suite_.addTest(TestCmdStreams('test_binary_uplink'))    
    return suite_


if __name__ == '__main__':
    _RUNNER = unittest.TextTestRunner(verbosity=2)
    _RUNNER.run((suite()))
