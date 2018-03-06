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

from pydoop.hdfs.common import parse_mode


class TestMode(unittest.TestCase):

    def runTest(self):
        for mode in "r", "rb":
            self.assertEqual(parse_mode(mode), ("r", False))
        for mode in "w", "wb":
            self.assertEqual(parse_mode(mode), ("w", False))
        for mode in "a", "ab":
            self.assertEqual(parse_mode(mode), ("a", False))
        self.assertEqual(parse_mode("rt"), ("r", True))
        self.assertEqual(parse_mode("wt"), ("w", True))
        self.assertEqual(parse_mode("at"), ("a", True))
        for mode in "", "k", "kb", "kt":
            self.assertRaises(ValueError, parse_mode, mode)


def suite():
    return unittest.TestLoader().loadTestsFromTestCase(TestMode)


if __name__ == '__main__':
    _RUNNER = unittest.TextTestRunner(verbosity=2)
    _RUNNER.run((suite()))
