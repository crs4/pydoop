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
import os
import shutil
import tempfile
import unittest
import uuid

import sercore


# TODO: from pydoop.test_utils import UNI_CHR
UNI_CHR = u'\N{CYRILLIC CAPITAL LETTER O WITH DIAERESIS}'


class TestFileInStream(unittest.TestCase):

    def setUp(self):
        with io.open(__file__, "rb") as f:
            self.data = f.read()
        self.stream = sercore.FileInStream()

    def test_from_path(self):
        self.stream.open(__file__)
        self.__check_stream()

    def test_from_file(self):
        with io.open(__file__, "rb") as f:
            self.stream.open(f)
            self.__check_stream()

    def test_errors(self):
        self.assertRaises(IOError, self.stream.open, uuid.uuid4().hex)
        self.stream.open(__file__)
        try:
            self.stream.skip(len(self.data))
            self.assertRaises(IOError, self.stream.read, 1)
        finally:
            self.stream.close()

    def __check_stream(self):
        try:
            self.assertEqual(self.stream.read(10), self.data[:10])
            self.stream.skip(20)
            self.assertEqual(self.stream.read(20), self.data[30:50])
        finally:
            self.stream.close()


class TestFileOutStream(unittest.TestCase):

    def setUp(self):
        self.wd = tempfile.mkdtemp(prefix="pydoop_")
        self.fname = os.path.join(self.wd, "foo")
        self.data = b"abcdefgh"
        self.stream = sercore.FileOutStream()

    def tearDown(self):
        shutil.rmtree(self.wd)

    def test_from_path(self):
        self.stream.open(self.fname)
        self.__fill_stream()
        self.__check_stream()

    def test_from_file(self):
        with io.open(self.fname, "wb") as f:
            self.stream.open(f)
            self.__fill_stream()
        self.__check_stream()

    def test_errors(self):
        fname = os.path.join(uuid.uuid4().hex, "foo")
        stream = sercore.FileOutStream()
        self.assertRaises(IOError, stream.open, fname)

    def __fill_stream(self):
        try:
            self.stream.write(self.data)
            self.stream.flush()
            self.stream.advance(10)
            self.stream.write(self.data)
        finally:
            self.stream.close()

    def __check_stream(self):
        with io.open(self.fname, "rb") as f:
            self.assertEqual(f.read(), self.data + 10 * b'\x00' + self.data)


class TestStringInStream(unittest.TestCase):

    def test_normal(self):
        data = b"abcdefgh"
        stream = sercore.StringInStream(data)
        self.assertEqual(stream.read(5), data[:5])
        self.assertEqual(stream.read(3), data[5:8])

    def test_oob(self):
        data = b"abc"
        for length in -1, 100:
            self.assertEqual(sercore.StringInStream(data).read(length), data)


class TestSerDe(unittest.TestCase):

    INT = 42
    LONG = (2 << 62) - 1
    FLOAT = 3.14
    STRING = u'BO' + UNI_CHR

    def setUp(self):
        self.wd = tempfile.mkdtemp(prefix="pydoop_")
        self.fname = os.path.join(self.wd, "foo")
        self.ostream = sercore.FileOutStream()
        self.istream = sercore.FileInStream()

    def tearDown(self):
        shutil.rmtree(self.wd)

    def test_int(self):
        self.ostream.open(self.fname)
        try:
            self.ostream.write_int(self.INT)
        finally:
            self.ostream.close()
        self.istream.open(self.fname)
        try:
            self.assertEqual(self.istream.read_int(), self.INT)
        finally:
            self.istream.close()

    def test_long(self):
        self.ostream.open(self.fname)
        try:
            self.ostream.write_long(self.LONG)
        finally:
            self.ostream.close()
        self.istream.open(self.fname)
        try:
            self.assertEqual(self.istream.read_long(), self.LONG)
        finally:
            self.istream.close()

    def test_float(self):
        self.ostream.open(self.fname)
        try:
            self.ostream.write_float(self.FLOAT)
        finally:
            self.ostream.close()
        self.istream.open(self.fname)
        try:
            self.assertAlmostEqual(self.istream.read_float(), self.FLOAT, 3)
        finally:
            self.istream.close()

    def test_string(self):
        self.ostream.open(self.fname)
        try:
            self.ostream.write_string(self.STRING)
        finally:
            self.ostream.close()
        self.istream.open(self.fname)
        self.assertEqual(self.istream.read_string(), self.STRING)

    def test_multi(self):
        self.ostream.open(self.fname)
        try:
            self.ostream.write_int(self.INT)
            self.ostream.write_long(self.LONG)
            self.ostream.write_float(self.FLOAT)
            self.ostream.write_string(self.STRING)
        finally:
            self.ostream.close()
        self.istream.open(self.fname)
        try:
            self.assertEqual(self.istream.read_int(), self.INT)
            self.assertEqual(self.istream.read_long(), self.LONG)
            self.assertAlmostEqual(self.istream.read_float(), self.FLOAT, 3)
            self.assertEqual(self.istream.read_string(), self.STRING)
        finally:
            self.istream.close()
        self.istream.open(self.fname)
        try:
            t = self.istream.read_tuple('ilfs')
            self.assertEqual(len(t), 4)
            self.assertEqual(t[0], self.INT)
            self.assertEqual(t[1], self.LONG)
            self.assertAlmostEqual(t[2], self.FLOAT, 3)
            self.assertEqual(t[3], self.STRING)
        finally:
            self.istream.close()


CASES = [
    TestFileInStream,
    TestFileOutStream,
    TestStringInStream,
    TestSerDe,
]


def suite():
    ret = unittest.TestSuite()
    test_loader = unittest.TestLoader()
    for c in CASES:
        ret.addTest(test_loader.loadTestsFromTestCase(c))
    return ret


if __name__ == '__main__':
    unittest.TextTestRunner(verbosity=2).run((suite()))
