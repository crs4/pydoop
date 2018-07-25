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
from pydoop.utils.py3compat import czip

import pydoop.mapreduce.streams as streams
from pydoop.mapreduce.text_streams import (TextWriter,
                                           TextDownStreamAdapter,
                                           TextUpStreamAdapter)
from pydoop.mapreduce.binary_streams import (BinaryWriter,
                                             BinaryDownStreamAdapter,
                                             BinaryUpStreamAdapter)

from pydoop.test_utils import WDTestCase

from data.stream_data import STREAM_3_DATA as STREAM_1


def stream_writer(fname, data, mod, Writer):
    with open(fname, 'w' + mod) as f:
        writer = Writer(f)
        for vals in data:
            writer.send(vals[0], *vals[1:])
        writer.flush()


def encode_strings(t):
    ret = []
    for item in t:
        try:
            ret.append(item.encode())
        except AttributeError:
            ret.append(item)
    return tuple(ret)


class TestCmdStreams(WDTestCase):

    def link_helper(self, mod, Writer, DownStreamAdapter):
        fname = self._mkfn('foo.' + ('bin' if mod == 'b' else 'txt'))
        stream_writer(fname, STREAM_1, mod, Writer)
        with open(fname, 'r' + mod) as f:
            commands = list(DownStreamAdapter(f))
        self.assertEqual(len(commands), len(STREAM_1))
        for in_t, t in czip(STREAM_1, commands):
            in_code, in_args = in_t[0], in_t[1:]
            self.assertEqual(len(t), 2)
            code, args = t
            self.assertEqual(code, in_code)
            self.assertEqual(len(args), len(in_args))
            if code == streams.SET_JOB_CONF:
                in_jc, jc = in_args[0], args[0]
                if mod == 'b':
                    in_jc = {k.encode(): v.encode() for k, v in in_jc.items()}
                self.assertEqual(jc, in_jc)
            else:
                if mod == 'b':
                    in_args = encode_strings(in_args)
                self.assertEqual(args, in_args)

    def test_text_downlink(self):
        self.link_helper('', TextWriter, TextDownStreamAdapter)

    def test_binary_downlink(self):
        self.link_helper('b', BinaryWriter, BinaryDownStreamAdapter)

    def test_text_uplink(self):
        self.link_helper('', TextUpStreamAdapter, TextDownStreamAdapter)

    def test_binary_uplink(self):
        self.link_helper('b', BinaryUpStreamAdapter, BinaryDownStreamAdapter)


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
