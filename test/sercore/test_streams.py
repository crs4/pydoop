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

import io
import os
import shutil
import struct
import tempfile
import unittest
import uuid
from random import randint

from pydoop.mapreduce.binary_protocol import OUTPUT, PARTITIONED_OUTPUT
import pydoop.sercore as sercore

INT64_MIN = -2**63
INT64_MAX = 2**63 - 1

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


class TestSerDe(unittest.TestCase):

    INT = 42
    LONG = INT64_MAX
    FLOAT = 3.14
    STRING = u'BO' + UNI_CHR
    BYTES = b'a\x00b'  # bytes r/w methods MUST preserve null characters
    TUPLE = INT, LONG, FLOAT, STRING, BYTES

    def setUp(self):
        self.wd = tempfile.mkdtemp(prefix="pydoop_")
        self.fname = os.path.join(self.wd, "foo")

    def tearDown(self):
        shutil.rmtree(self.wd)

    def test_vint(self):
        with sercore.FileOutStream(self.fname) as s:
            s.write_vint(self.INT)
        with sercore.FileInStream(self.fname) as s:
            self.assertEqual(s.read_vint(), self.INT)

    def test_vlong(self):
        with sercore.FileOutStream(self.fname) as s:
            s.write_vlong(self.LONG)
        with sercore.FileInStream(self.fname) as s:
            self.assertEqual(s.read_vlong(), self.LONG)

    def test_float(self):
        with sercore.FileOutStream(self.fname) as s:
            s.write_float(self.FLOAT)
        with sercore.FileInStream(self.fname) as s:
            self.assertAlmostEqual(s.read_float(), self.FLOAT, 3)

    def test_string_as_string(self):
        with sercore.FileOutStream(self.fname) as s:
            s.write_string(self.STRING)
        with sercore.FileInStream(self.fname) as s:
            self.assertEqual(s.read_string(), self.STRING)

    def test_string_as_bytes(self):
        with sercore.FileOutStream(self.fname) as s:
            s.write_string(self.STRING)
        with sercore.FileInStream(self.fname) as s:
            self.assertEqual(s.read_bytes(), self.STRING.encode("utf8"))

    def test_bytes_as_string(self):
        with sercore.FileOutStream(self.fname) as s:
            s.write_bytes(self.BYTES)
        with sercore.FileInStream(self.fname) as s:
            self.assertEqual(s.read_string(), self.BYTES.decode("utf8"))

    def test_bytes_as_bytes(self):
        with sercore.FileOutStream(self.fname) as s:
            s.write_bytes(self.BYTES)
        with sercore.FileInStream(self.fname) as s:
            self.assertEqual(s.read_bytes(), self.BYTES)

    def test_output(self):
        k, v = b"key", b"value"
        with sercore.FileOutStream(self.fname) as s:
            s.write_output(k, v)
        with sercore.FileInStream(self.fname) as s:
            self.assertEqual(s.read_vint(), OUTPUT)
            self.assertEqual(s.read_bytes(), k)
            self.assertEqual(s.read_bytes(), v)
        part = 1
        with sercore.FileOutStream(self.fname) as s:
            s.write_output(k, v, part)
        with sercore.FileInStream(self.fname) as s:
            self.assertEqual(s.read_vint(), PARTITIONED_OUTPUT)
            self.assertEqual(s.read_vint(), part)
            self.assertEqual(s.read_bytes(), k)
            self.assertEqual(s.read_bytes(), v)

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
            s.write_vint(self.INT)
            s.write_vlong(self.LONG)
            s.write_float(self.FLOAT)
            s.write_string(self.STRING)
            s.write_bytes(self.BYTES)

    def __fill_stream_tuple(self):
        with sercore.FileOutStream(self.fname) as s:
            s.write_tuple("ilfsb", self.TUPLE)

    def __check_stream_multi(self):
        with sercore.FileInStream(self.fname) as s:
            self.assertEqual(s.read_vint(), self.INT)
            self.assertEqual(s.read_vlong(), self.LONG)
            self.assertAlmostEqual(s.read_float(), self.FLOAT, 3)
            self.assertEqual(s.read_string(), self.STRING)
            self.assertEqual(s.read_bytes(), self.BYTES)

    def __check_stream_tuple(self):
        with sercore.FileInStream(self.fname) as s:
            t = s.read_tuple('ilfsb')
            self.assertEqual(len(t), 5)
            self.assertEqual(t[0], self.INT)
            self.assertEqual(t[1], self.LONG)
            self.assertAlmostEqual(t[2], self.FLOAT, 3)
            self.assertEqual(t[3], self.STRING)
            self.assertEqual(t[4], self.BYTES)

    def test_errors(self):
        type_mismatches = [
            ("vint", 1.), ("vint", "x"), ("vint", b"x"),
            ("vlong", 1.), ("vlong", "x"), ("vlong", b"x"),
            ("float", "x"), ("float", b"x"),
            ("bytes", 1), ("bytes", 1.), ("bytes", u"x"),
            ("string", 1), ("string", 1.),
        ]
        with sercore.FileOutStream(self.fname) as s:
            for name, val in type_mismatches:
                meth = getattr(s, "write_%s" % name)
                self.assertRaises(TypeError, meth, val)
        self.__fill_stream_tuple()
        with sercore.FileInStream(self.fname) as s:
            with self.assertRaises(IOError):
                s.read_tuple("ilfsbi")  # EOF
        with sercore.FileOutStream(self.fname) as s:
            with self.assertRaises(ValueError):
                s.write_tuple("iis", (1, 2))  # not enough items

    # "extra" features

    def test_string_keep_zeros(self):
        pystr = self.BYTES.decode("utf-8")
        with sercore.FileOutStream(self.fname) as s:
            s.write_string(pystr)
        with sercore.FileInStream(self.fname) as s:
            val = s.read_bytes()
            self.assertEqual(val, self.BYTES)

    def test_string_allow_bytes(self):
        with sercore.FileOutStream(self.fname) as s:
            s.write_string(self.BYTES)
        with sercore.FileInStream(self.fname) as s:
            self.assertEqual(s.read_bytes(), self.BYTES)


class TestCheckClosed(unittest.TestCase):

    def test_instream(self):
        with sercore.FileInStream(__file__) as stream:
            pass
        ops = (
            (stream.read, (1,)),
            (stream.read_vint, ()),
            (stream.read_vlong, ()),
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
            (stream.write_vint, (1,)),
            (stream.write_vlong, (1,)),
            (stream.write_float, (1.0,)),
            (stream.write_string, (u"x")),
            (stream.write_tuple, ("ii", (1, 1))),
            (stream.advance, (1,)),
            (stream.flush, ()),
        )
        self.__check(ops)
        shutil.rmtree(wd)

    def test_double_close(self):
        wd = tempfile.mkdtemp(prefix="pydoop_")
        fname = os.path.join(wd, "foo")
        with sercore.FileOutStream(fname) as stream:
            pass
        stream.close()
        with sercore.FileInStream(fname) as stream:
            pass
        stream.close()
        with io.open(fname, "wb") as f:
            with sercore.FileOutStream(f) as stream:
                pass
            stream.close()
        with io.open(fname, "rb") as f:
            with sercore.FileInStream(f) as stream:
                pass
            stream.close()

    def __check(self, ops):
        for o, args in ops:
            self.assertRaises(ValueError, o, *args)


class TestHadoopTypes(unittest.TestCase):

    def setUp(self):
        self.wd = tempfile.mkdtemp(prefix="pydoop_")
        self.fname = os.path.join(self.wd, "foo")

    def test_long_writable(self):
        preset_data = (INT64_MIN, -100, -1, 0, 1, 100, INT64_MAX)
        random_data = [randint(INT64_MIN, INT64_MAX) for _ in range(100)]
        for data in preset_data, random_data:
            with io.open(self.fname, "wb") as f:
                f.write(struct.pack(">" + len(data) * "q", *data))
            with sercore.FileInStream(self.fname) as stream:
                for v in data:
                    self.assertEqual(stream.read_long_writable(), v)
        # payload entry, e.g., TextInputFormat key
        k = 1000
        sk = struct.pack(">q", k)
        with sercore.FileOutStream(self.fname) as stream:
            stream.write_bytes(sk)
        with sercore.FileInStream(self.fname) as stream:
            self.assertEqual(stream.read_vint(), len(sk))
            self.assertEqual(stream.read_long_writable(), k)


CASES = [
    TestFileInStream,
    TestFileOutStream,
    TestSerDe,
    TestCheckClosed,
    TestHadoopTypes,
]


def suite():
    ret = unittest.TestSuite()
    test_loader = unittest.TestLoader()
    for c in CASES:
        ret.addTest(test_loader.loadTestsFromTestCase(c))
    return ret


if __name__ == '__main__':
    unittest.TextTestRunner(verbosity=2).run((suite()))
