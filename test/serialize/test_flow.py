# BEGIN_COPYRIGHT
#
# Copyright 2009-2018 CRS4.
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

import unittest
from pydoop.utils.serialize import FlowWriter, FlowReader
from pydoop.test_utils import WDTestCase


class TestFlow(WDTestCase):
    def write_read_helper(self, N, rule, data):
        fname = 'foo.dat'  # self._mkfn('foo.dat')
        with FlowWriter(open(fname, 'wb')) as stream:
            for _ in range(N):
                stream.write(rule, data)
        with FlowReader(open(fname, 'rb')) as stream:
            for _ in range(N):
                xdata = stream.read(rule)
                self.assertEqual(len(xdata), len(data))
                for x, y in zip(xdata, data):
                    if isinstance(x, float):
                        self.assertAlmostEqual(x, y)
                    else:
                        self.assertEqual(x, y)

    def test_write_read_basic(self):
        rule = b'sfis'
        data = (b'A string', 0.33, 22233, b'Another String')
        N = 10
        self.write_read_helper(N, rule, data)

    def test_write_read_list_of_strings(self):
        rule = b'A'
        data = ((b'aaa', b'bbb', b'ccc', b'ddd'),)
        N = 10
        self.write_read_helper(N, rule, data)

    def test_write_read_mixed(self):
        rule = b'fAsiisS'
        data = (0.54, (b'aaa', b'bbb', b'ccc', b'ddd'),
                b'ssed', 4343, 35555, b'weweew', b'ooioioi')
        N = 10
        self.write_read_helper(N, rule, data)


def suite():
    return unittest.TestLoader().loadTestsFromTestCase(TestFlow)


if __name__ == '__main__':
    _RUNNER = unittest.TextTestRunner(verbosity=2)
    _RUNNER.run((suite()))
