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

    def test_from_path(self):
        with sercore.FileInStream(__file__) as s:
            self.__check_stream(s)

    def test_from_file(self):
        with io.open(__file__, "rb") as f:
            with sercore.FileInStream(f) as s:
                self.__check_stream(s)

    def test_errors(self):
        with self.assertRaises(IOError):
            sercore.FileInStream(uuid.uuid4().hex)
        with sercore.FileInStream(__file__) as s:
            s.skip(len(self.data))
            with self.assertRaises(IOError):
                s.read(1)

    def __check_stream(self, s):
        self.assertEqual(s.read(10), self.data[:10])
        s.skip(20)
        self.assertEqual(s.read(20), self.data[30:50])


class TestFileOutStream(unittest.TestCase):

    def setUp(self):
        self.wd = tempfile.mkdtemp(prefix="pydoop_")
        self.fname = os.path.join(self.wd, "foo")
        self.data = b"abcdefgh"

    def tearDown(self):
        shutil.rmtree(self.wd)

    def test_from_path(self):
        with sercore.FileOutStream(self.fname) as s:
            self.__fill_stream(s)
        self.__check_stream()

    def test_from_file(self):
        with io.open(self.fname, "wb") as f:
            with sercore.FileOutStream(f) as s:
                self.__fill_stream(s)
        self.__check_stream()

    def test_errors(self):
        with self.assertRaises(IOError):
            sercore.FileOutStream(os.path.join(uuid.uuid4().hex, "foo"))

    def __fill_stream(self, s):
        s.write(self.data)
        s.flush()
        s.advance(10)
        s.write(self.data)

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
    TUPLE = INT, LONG, FLOAT, STRING, STRING

    def setUp(self):
        self.wd = tempfile.mkdtemp(prefix="pydoop_")
        self.fname = os.path.join(self.wd, "foo")

    def tearDown(self):
        shutil.rmtree(self.wd)

    def test_int(self):
        with sercore.FileOutStream(self.fname) as s:
            s.write_int(self.INT)
        with sercore.FileInStream(self.fname) as s:
            self.assertEqual(s.read_int(), self.INT)

    def test_long(self):
        with sercore.FileOutStream(self.fname) as s:
            s.write_long(self.LONG)
        with sercore.FileInStream(self.fname) as s:
            self.assertEqual(s.read_long(), self.LONG)

    def test_float(self):
        with sercore.FileOutStream(self.fname) as s:
            s.write_float(self.FLOAT)
        with sercore.FileInStream(self.fname) as s:
            self.assertAlmostEqual(s.read_float(), self.FLOAT, 3)

    def test_string(self):
        with sercore.FileOutStream(self.fname) as s:
            s.write_string(self.STRING)
        with sercore.FileInStream(self.fname) as s:
            self.assertEqual(s.read_string(), self.STRING)

    def test_bytes(self):
        with sercore.FileOutStream(self.fname) as s:
            s.write_string(self.STRING)
        with sercore.FileInStream(self.fname) as s:
            self.assertEqual(s.read_bytes(), self.STRING.encode("utf8"))

    def test_multi_no_tuple(self):
        self.__fill_stream_multi()
        self.__check_stream_multi()

    def test_multi_read_tuple(self):
        self.__fill_stream_multi()
        self.__check_stream_tuple()

    def test_multi_write_tuple(self):
        self.__fill_stream_tuple()
        self.__check_stream_multi()

    def test_multi_rw_tuple(self):
        self.__fill_stream_tuple()
        self.__check_stream_tuple()

    def __fill_stream_multi(self):
        with sercore.FileOutStream(self.fname) as s:
            s.write_int(self.INT)
            s.write_long(self.LONG)
            s.write_float(self.FLOAT)
            s.write_string(self.STRING)
            s.write_string(self.STRING)

    def __fill_stream_tuple(self):
        with sercore.FileOutStream(self.fname) as s:
            s.write_tuple(self.TUPLE, 'ilfss')

    def __check_stream_multi(self):
        with sercore.FileInStream(self.fname) as s:
            self.assertEqual(s.read_int(), self.INT)
            self.assertEqual(s.read_long(), self.LONG)
            self.assertAlmostEqual(s.read_float(), self.FLOAT, 3)
            self.assertEqual(s.read_string(), self.STRING)
            self.assertEqual(s.read_bytes(), self.STRING.encode("utf8"))

    def __check_stream_tuple(self):
        with sercore.FileInStream(self.fname) as s:
            t = s.read_tuple('ilfsb')
            self.assertEqual(len(t), 5)
            self.assertEqual(t[0], self.INT)
            self.assertEqual(t[1], self.LONG)
            self.assertAlmostEqual(t[2], self.FLOAT, 3)
            self.assertEqual(t[3], self.STRING)
            self.assertEqual(t[4], self.STRING.encode("utf8"))

    def test_errors(self):
        self.__fill_stream_tuple()
        with sercore.FileInStream(self.fname) as s:
            with self.assertRaises(IOError):
                s.read_tuple("ilfsbi")  # EOF
        with sercore.FileOutStream(self.fname) as s:
            with self.assertRaises(ValueError):
                s.write_tuple((1, 2), "iis")  # not enough items
        with sercore.FileOutStream(self.fname) as s:
            with self.assertRaises(TypeError):
                s.write_tuple((1, 2.1), "ii")


class TestCheckClosed(unittest.TestCase):

    def test_instream(self):
        with sercore.FileInStream(__file__) as stream:
            pass
        ops = (
            (stream.read, (1,)),
            (stream.read_int, ()),
            (stream.read_long, ()),
            (stream.read_float, ()),
            (stream.read_string, ()),
            (stream.read_tuple, ("ii")),
            (stream.skip, (1,)),
        )
        self.__check(ops)

    def test_outstream(self):
        wd = tempfile.mkdtemp(prefix="pydoop_")
        fname = os.path.join(wd, "foo")
        with sercore.FileOutStream(fname) as stream:
            pass
        ops = (
            (stream.write, (b"x",)),
            (stream.write_int, (1,)),
            (stream.write_long, (1,)),
            (stream.write_float, (1.0,)),
            (stream.write_string, ("x")),
            (stream.write_tuple, ("ii")),
            (stream.advance, (1,)),
            (stream.flush, ()),
        )
        self.__check(ops)
        shutil.rmtree(wd)

    def __check(self, ops):
        for o, args in ops:
            self.assertRaises(ValueError, o, *args)


CASES = [
    TestFileInStream,
    TestFileOutStream,
    TestStringInStream,
    TestSerDe,
    TestCheckClosed,
]


def suite():
    ret = unittest.TestSuite()
    test_loader = unittest.TestLoader()
    for c in CASES:
        ret.addTest(test_loader.loadTestsFromTestCase(c))
    return ret


if __name__ == '__main__':
    unittest.TextTestRunner(verbosity=2).run((suite()))
