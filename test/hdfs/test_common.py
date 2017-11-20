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
import itertools

from pydoop.hdfs.common import Mode


class TestMode(unittest.TestCase):

    def test_args(self):
        keys = ("arg", "value", "flags", "binary", "writable")
        for t in [
            ("r", "rb", os.O_RDONLY, True, False),
            ("rt", "rt", os.O_RDONLY, False, False),
            ("rb", "rb", os.O_RDONLY, True, False),
            (os.O_RDONLY, "rb", os.O_RDONLY, True, False),
            ("w", "wb", os.O_WRONLY, True, True),
            ("wt", "wt", os.O_WRONLY, False, True),
            ("wb", "wb", os.O_WRONLY, True, True),
            (os.O_WRONLY, "wb", os.O_WRONLY, True, True),
            ("a", "ab", os.O_WRONLY | os.O_APPEND, True, True),
            ("at", "at", os.O_WRONLY | os.O_APPEND, False, True),
            ("ab", "ab", os.O_WRONLY | os.O_APPEND, True, True),
            (os.O_WRONLY | os.O_APPEND, "ab", os.O_WRONLY | os.O_APPEND,
             True, True),
        ]:
            d = dict(zip(keys, t))
            m = Mode(d.pop('arg'))
            for k, v in d.items():
                self.assertEqual(getattr(m, k), v)
            self.assertEqual(str(m), d["value"])

    def test_default(self):
        m = Mode()
        self.assertEqual(m.value, "rb")
        self.assertEqual(m.flags, os.O_RDONLY)
        self.assertTrue(m.binary)

    def test_exceptions(self):
        for arg in (-1, "k", [0]):
            self.assertRaises(ValueError, Mode, arg)

    def test_eq(self):
        m1, m2 = Mode(), Mode()
        self.assertEqual(m1, m2)
        values = "r", "w", "a", "rb", "wb", "ab", "rt", "wt", "at"
        equiv = set([("r", "rb"), ("w", "wb"), ("a", "ab")])
        for v in values:
            self.assertEqual(Mode(v), Mode(v))
            self.assertEqual(Mode(v), v)
        for v1, v2 in itertools.combinations(values, 2):
            if (v1, v2) in equiv:
                self.assertEqual(Mode(v1), Mode(v2))
                self.assertEqual(Mode(v1), v2)
                self.assertEqual(Mode(v2), v1)
            else:
                self.assertNotEqual(Mode(v1), Mode(v2))
                self.assertNotEqual(Mode(v1), v2)
                self.assertNotEqual(Mode(v2), v1)

    def test_copy(self):
        m = Mode()
        for cp in Mode(m), Mode.copy(m):
            self.assertFalse(cp is m)
            self.assertEqual(cp, m)


def suite():
    return unittest.TestLoader().loadTestsFromTestCase(TestMode)


if __name__ == '__main__':
    _RUNNER = unittest.TextTestRunner(verbosity=2)
    _RUNNER.run((suite()))
