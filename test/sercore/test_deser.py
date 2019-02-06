# BEGIN_COPYRIGHT
#
# Copyright 2009-2019 CRS4.
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

import os
import shutil
import struct
import tempfile
import unittest

import pydoop.sercore as sercore


class TestFileSplit(unittest.TestCase):

    def setUp(self):
        work_dir = tempfile.mkdtemp(prefix="pydoop_")
        work_path = os.path.join(work_dir, "foo")
        self.filename, self.offset, self.length = "foobar", 0, 100
        with sercore.FileOutStream(work_path) as s:
            s.write_string(self.filename)
            s.write(struct.pack(">q", self.offset))
            s.write(struct.pack(">q", self.length))
        size = os.stat(work_path).st_size
        with sercore.FileInStream(work_path) as s:
            self.raw_split = s.read(size)
        shutil.rmtree(work_dir)

    def test_standard(self):
        t = sercore.deserialize_file_split(self.raw_split)
        self.assertEqual(len(t), 3)
        self.assertEqual(t[0], self.filename)
        self.assertEqual(t[1], self.offset)
        self.assertEqual(t[2], self.length)

    def test_errors(self):
        with self.assertRaises(IOError):
            sercore.deserialize_file_split(self.raw_split[:-1])


CASES = [
    TestFileSplit,
]


def suite():
    ret = unittest.TestSuite()
    test_loader = unittest.TestLoader()
    for c in CASES:
        ret.addTest(test_loader.loadTestsFromTestCase(c))
    return ret


if __name__ == '__main__':
    unittest.TextTestRunner(verbosity=2).run((suite()))
