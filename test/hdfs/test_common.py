# BEGIN_COPYRIGHT
#
# Copyright 2009-2017 CRS4.
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
import os

from pydoop.hdfs.common import Mode


class TestHDFSCommon(unittest.TestCase):

    def test_mode(self):
        keys = ("arg", "value", "flags", "binary")
        for t in [
            ("r", "r", os.O_RDONLY, False),
            ("rt", "r", os.O_RDONLY, False),
            ("rb", "r", os.O_RDONLY, True),
            (os.O_RDONLY, "r", os.O_RDONLY, False),
            ("w", "w", os.O_WRONLY, False),
            ("wt", "w", os.O_WRONLY, False),
            ("wb", "w", os.O_WRONLY, True),
            (os.O_WRONLY, "w", os.O_WRONLY, False),
            ("a", "a", os.O_WRONLY | os.O_APPEND, False),
            ("at", "a", os.O_WRONLY | os.O_APPEND, False),
            ("ab", "a", os.O_WRONLY | os.O_APPEND, True),
            (os.O_WRONLY | os.O_APPEND, "a", os.O_WRONLY | os.O_APPEND, False),
        ]:
            d = dict(zip(keys, t))
            m = Mode(d.pop('arg'))
            for k, v in d.items():
                self.assertEqual(getattr(m, k), v)
        # default
        m = Mode()
        self.assertEqual(m.value, "r")
        self.assertEqual(m.flags, os.O_RDONLY)
        self.assertFalse(m.binary)
        # exceptions
        for arg in (-1, "k", [0]):
            self.assertRaises(ValueError, Mode, arg)


def suite():
    suite_ = unittest.TestSuite()
    suite_.addTest(TestHDFSCommon("test_mode"))
    return suite_


if __name__ == '__main__':
    _RUNNER = unittest.TextTestRunner(verbosity=2)
    _RUNNER.run((suite()))
