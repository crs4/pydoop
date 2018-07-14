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

import io
import unittest
import uuid

import sercore


class TestFileInStream(unittest.TestCase):

    def test_normal(self):
        with io.open(__file__, "rb") as f:
            self.data = f.read()
        stream = sercore.FileInStream()
        stream.open(__file__)
        self.assertEqual(stream.read(10), self.data[:10])
        stream.skip(20)
        self.assertEqual(stream.read(20), self.data[30:50])
        stream.close()

    def test_errors(self):
        stream = sercore.FileInStream()
        self.assertRaises(IOError, stream.open, uuid.uuid4().hex)


def suite():
    return unittest.TestLoader().loadTestsFromTestCase(TestFileInStream)


if __name__ == '__main__':
    unittest.TextTestRunner(verbosity=2).run((suite()))
