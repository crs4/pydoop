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

import unittest
import itertools as it

from pydoop.mapreduce.streams import ProtocolError
from pydoop.mapreduce.text_streams import TextDownStreamFilter
from pydoop.test_utils import WDTestCase


STREAM_1 = [
    ('start', 0),
    ('setJobConf', 'key1', 'value1', 'key2', 'value2'),
    ('setInputTypes', 'key_type', 'value_type'),
    ('runMap', 'input_split', 3, False),
    ('mapItem', 'key1', 'val1'),
    ('mapItem', 'key1', 'val2'),
    ('mapItem', 'key2', 'val3'),
    ('runReduce', 0, False),
    ('reduceKey', 'key1'),
    ('reduceValue', 'val1'),
    ('reduceValue', 'val2'),
    ('reduceKey', 'key2'),
    ('reduceValue', 'val3'),
    ('close',),
    ]


def stream_writer(fname, data):
    with open(fname, 'w') as f:
        for vals in data:
            f.write('\t'.join(map(str, vals)) + '\n')


class TestTextStream(WDTestCase):

    def test_downlink(self):
        fname = self._mkfn('foo.txt')
        stream_writer(fname, STREAM_1)
        with open(fname, 'r') as f:
            stream = TextDownStreamFilter(f)
            try:
                for (cmd, args), vals in it.izip(stream, STREAM_1):
                    self.assertEqual(cmd, vals[0])
                    self.assertTrue((len(vals) == 1 and not args)
                                    or (vals[1:] == args))
            except ProtocolError as e:
                print 'error -- %s' % e


def suite():
    suite_ = unittest.TestSuite()
    suite_.addTest(TestTextStream('test_downlink'))
    return suite_


if __name__ == '__main__':
    _RUNNER = unittest.TextTestRunner(verbosity=2)
    _RUNNER.run((suite()))
