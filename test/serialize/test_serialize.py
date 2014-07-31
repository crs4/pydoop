
# vim: set fileencoding: utf-8

# BEGIN_COPYRIGHT
#
# Copyright 2009-2014 CRS4.
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


# to compile the Java program used by test_deserializing_java_output:
#
#    javac -cp $(hadoop classpath) hadoop_serialize.java

import unittest, StringIO, random
import os
import subprocess

#FIXME
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../'))

import pydoop
import pydoop.mapreduce.serialize as srl
import pydoop.mapreduce.jwritable_utils as wu

HadoopSerializeClass = 'hadoop_serialize'

class TestSerialize(unittest.TestCase):

    def setUp(self):
        self.stream = StringIO.StringIO()

    def test_int(self):
        stream = self.stream
        for i in range(-16782,16782):
            srl.serialize_vint(i, stream)
        stream.seek(0)
        for i in range(-16782,16782):
            x = srl.deserialize_vint(stream)
            self.assertEqual(i, x)

    def test_int_big(self):
        stream = self.stream
        numbers = random.sample(xrange(-18999289888, 18999289888), 10000)
        for i in numbers:
            srl.serialize_vint(i, stream)
        stream.seek(0)
        for i in numbers:
            x = srl.deserialize_vint(stream)
            self.assertEqual(i, x)

    def test_float(self):
        stream = self.stream
        numbers = [random.uniform(-100000, 100000) for _ in range(10000)]
        for f in numbers:
            srl.serialize_float(f, stream)
        stream.seek(0)
        for f in numbers:
            x = srl.deserialize_float(stream)
            # be paranoid...
            if abs(x+f) == 0:
                self.assertTrue(abs(f-x) < 1e-6)
            else:
                self.assertTrue(abs(f-x)/abs(x+f) < 1e-6)

    def test_string(self):
        N = 10
        stream = self.stream
        with open(__file__) as f:
            s = unicode(f.read(), 'utf-8')
        t = s
        for _ in range(N):
            srl.serialize_text(t, stream)
        stream.seek(0)
        t = s
        for _ in range(N):
            s1 = srl.deserialize_text(stream)
            self.assertEqual(t, s1)

    def test_mixture(self):
        stream = self.stream
        vals = [1, 0.33, 0.3290, 1902, 'sshjdhsj', 0.3, -33, 'ueiwriuqrei']
        for v in vals:
            if isinstance(v, int):
                srl.serialize_int(v, stream)
            elif isinstance(v, float):
                srl.serialize_float(v, stream)
            elif isinstance(v, str):
                srl.serialize_text(v, stream)
        stream.seek(0)
        for v in vals:
            if isinstance(v, int):
                x = srl.deserialize_int(stream)
                self.assertEqual(v, x)
            elif isinstance(v, float):
                x = srl.deserialize_float(stream)
                self.assertTrue(abs(v-x)/abs(v+x) < 1e-6)
            elif isinstance(v, str):
                x = srl.deserialize_text(stream)
                self.assertEqual(v, x)

    def test_deserializing_java_output(self):
        byte_stream = _get_java_output_stream()

        # read integers
        self.assertEqual(42, wu.readVInt(byte_stream))
        self.assertEqual(4242, wu.readVInt(byte_stream))
        self.assertEqual(424242, wu.readVInt(byte_stream))
        self.assertEqual(42424242, wu.readVInt(byte_stream))
        self.assertEqual(-42, wu.readVInt(byte_stream))

        # longs
        self.assertEqual(42, wu.readVLong(byte_stream))
        self.assertEqual(424242, wu.readVLong(byte_stream))
        self.assertEqual(4242424242, wu.readVLong(byte_stream))

        # strings
        # first one is plain ASCII
        self.assertEqual(u"hello world", wu.readString(byte_stream))
        # second has accented characters
        self.assertEqual(u"oggi è giovedì", wu.readString(byte_stream))

        # final piece is an encoded Text object
        self.assertEqual(u"à Text object", srl.deserialize_text(byte_stream))

    def test_wu_ascii_string(self):
        # test for self-consistency
        wu.writeString(self.stream, "simple")
        self.stream.seek(0)
        self.assertEqual(u"simple", wu.readString(self.stream))

    def test_wu_nonascii_string(self):
        # test for self-consistency
        wu.writeString(self.stream, u"àéìòù")
        self.stream.seek(0)
        self.assertEqual(u"àéìòù", wu.readString(self.stream))

    def test_wu_ints(self):
        # test for self-consistency
        wu.writeVInt(self.stream, 42)
        wu.writeVLong(self.stream, 4000000000)
        self.stream.seek(0)
        self.assertEqual(42, wu.readVInt(self.stream))
        self.assertEqual(4000000000, wu.readVLong(self.stream))

def _get_java_output_stream():
    this_directory = os.path.abspath(os.path.dirname(__file__))
    classpath = '.:%s' % pydoop.hadoop_classpath()
    output = subprocess.check_output(
            ['java', '-cp', classpath, HadoopSerializeClass],
            cwd=this_directory,
            stderr=open('/dev/null', 'w'))
    stream = StringIO.StringIO(output)
    return stream


def suite():
    return unittest.TestLoader().loadTestsFromTestCase(TestSerialize)


if __name__ == '__main__':
    runner = unittest.TextTestRunner(verbosity=2)
    runner.run((suite()))
