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

from data.stream_data import STREAM_3_DATA as STREAM_1


def stream_writer(fname, data, mod, Writer):
    with open(fname, 'w' + mod) as f:
        writer = Writer(f)
        for vals in data:
            writer.send(vals[0], *vals[1:])
        writer.flush()


class TestCmdStreams(WDTestCase):

    def link_helper(self, mod, Writer, DownStreamAdapter):
        fname = self._mkfn('foo.' + ('bin' if mod == 'b' else 'txt'))
        stream_writer(fname, STREAM_1, mod, Writer)
        with open(fname, 'r' + mod) as f:
            stream = DownStreamAdapter(f)
            try:
                at_least_once_in_loop = False
                for (cmd, args), vals in czip(stream, STREAM_1):
                    at_least_once_in_loop = True
                    self.assertEqual(cmd, vals[0])
                    vals = vals[1:]
                    if mod == 'b':
                        vals = [x.encode('utf-8')
                                if isinstance(x, unicode) else x
                                for x in vals]
                    vals = tuple(vals)
                    if cmd == streams.SET_JOB_CONF:
                        self.assertEqual(len(args), 1)
                        self.assertEqual(args[0], vals)
                    else:
                        self.assertTrue((len(vals) == 0 and not args) or
                                        (vals == args))
                self.assertTrue(at_least_once_in_loop)
            except ProtocolError as e:
                print('error -- %s' % e)

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
