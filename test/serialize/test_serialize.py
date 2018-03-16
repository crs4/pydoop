# -*- coding: utf-8 -*-
# vim: set fileencoding: utf-8

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


# to compile the Java program used by test_deserializing_java_output:
#
#    javac -cp $(hadoop classpath) hadoop_serialize.java

import unittest
import os
import subprocess
import tempfile
import shutil

import pydoop
from pydoop.utils.py3compat import StringIO, basestring
import pydoop.mapreduce.jwritable_utils as wu
import pydoop.utils.serialize as srl


_HADOOP_SERIALIZE_CLASS = 'hadoop_serialize'


class TestSerialization(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass


class TestSerialize(unittest.TestCase):

    def setUp(self):
        self.stream = StringIO()
        self.wd = tempfile.mkdtemp(prefix="pydoop_")

    def tearDown(self):
            shutil.rmtree(self.wd)

    def test_deserializing_java_output_1(self):
        try:
            byte_stream = _get_java_output_stream(self.wd)

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
            self.assertEqual(
                u"à Text object", srl.deserialize_text(byte_stream)
            )
        finally:
            pass

    def test_deserializing_java_output_2(self):
        try:
            byte_stream = _get_java_output_stream(self.wd)

            # read integers
            self.assertEqual(42, srl.deserialize_vint(byte_stream))
            self.assertEqual(4242, srl.deserialize_vint(byte_stream))
            self.assertEqual(424242, srl.deserialize_vint(byte_stream))
            self.assertEqual(42424242, srl.deserialize_vint(byte_stream))
            self.assertEqual(-42, srl.deserialize_vint(byte_stream))

            # longs
            self.assertEqual(42, srl.deserialize_vint(byte_stream))
            self.assertEqual(424242, srl.deserialize_vint(byte_stream))
            self.assertEqual(4242424242, srl.deserialize_vint(byte_stream))

            # strings
            # first one is plain ASCII
            self.assertEqual(u"hello world", wu.readString(byte_stream))
            # second has accented characters
            self.assertEqual(u"oggi è giovedì", wu.readString(byte_stream))

            # final piece is an encoded Text object
            self.assertEqual(
                u"à Text object", srl.deserialize_text(byte_stream)
            )
        finally:
            pass

    def test_deserializing_java_output_3(self):
        try:
            byte_stream = _get_java_output_stream(self.wd)
            fname = os.path.join(self.wd, 'foo.dat')
            with open(fname, 'wb') as f:
                f.write(byte_stream.getvalue())
            with srl.FlowReader(open(fname, 'rb')) as flow:
                # read integers
                self.assertEqual(42, flow.read("i")[0])
                self.assertEqual(4242, flow.read("i")[0])
                self.assertEqual(424242, flow.read("i")[0])
                self.assertEqual(42424242, flow.read("i")[0])
                self.assertEqual(-42, flow.read("i")[0])
                # longs
                self.assertEqual(42, flow.read("L")[0])
                self.assertEqual(424242, flow.read("L")[0])
                self.assertEqual(4242424242, flow.read("L")[0])
                # strings
                # first one is plain ASCII
                self.assertEqual(u"hello world",
                                 flow.read("S")[0].decode('UTF-8'))
                # second has accented characters
                self.assertEqual(u"oggi è giovedì",
                                 flow.read("S")[0].decode('UTF-8'))
                # final piece is an encoded Text object
                self.assertEqual(
                    u"à Text object", flow.read("s")[0].decode('UTF-8')
                )
        finally:
            pass

    def test_simulate_java_output_1(self):
        try:
            byte_stream = _get_java_output_stream(self.wd)
            out_stream = StringIO()
            # write integers
            srl.serialize_vint(42, out_stream)
            srl.serialize_vint(4242, out_stream)
            srl.serialize_vint(424242, out_stream)
            srl.serialize_vint(42424242, out_stream)
            srl.serialize_vint(-42, out_stream)
            # write longs
            srl.serialize_vint(42, out_stream)
            srl.serialize_vint(424242, out_stream)
            srl.serialize_vint(4242424242, out_stream)
            # strings
            wu.writeString(out_stream, u"hello world")
            # second has accented characters
            wu.writeString(out_stream, u"oggi è giovedì")
            #
            srl.serialize_text(u"à Text object", out_stream)
            self.assertEqual(byte_stream.getvalue(), out_stream.getvalue())
        finally:
            pass

    def srl_helper(self, data, rule=None):
        fname = os.path.join(self.wd, 'foo.dat')
        with srl.FlowWriter(open(fname, 'wb')) as flow:
            if rule is None:
                rule = ''.join(['L' if isinstance(x, int) else 'S'
                                for x in data])
            flow.write(rule.encode('UTF-8'), data)
        with open(fname, 'rb') as f:
            return f.read()

    def wu_helper(self, data):
        out_stream = StringIO()
        for x in data:
            if isinstance(x, int):
                srl.serialize_vint(x, out_stream)
            elif isinstance(x, basestring):
                wu.writeString(out_stream, x)
        return out_stream.getvalue()

    def test_write_equiv(self):
        data = (42, 4242, 424242, 42424242, -42,
                42, 424242, 42424242424242,
                u"hello world", u"oggi è giovedì", u"à Text object")
        ser0 = self.srl_helper(data)
        ser1 = self.wu_helper(data)
        self.assertEqual(ser0, ser1)

    def test_simulate_java_output_2(self):
        data = (42, 4242, 424242, 42424242, -42,
                42, 424242, 4242424242,
                u"hello world", u"oggi è giovedì", u"à Text object")
        rule = "iiiiiLLLSSs"
        try:
            ser0 = _get_java_output_stream(self.wd).getvalue()
            ser1 = self.srl_helper(data, rule)
            self.assertEqual(ser0, ser1)
        finally:
            pass

    def test_serialize_old_style_filename(self):
        fn = 'some_filename.file'
        srl.serialize_old_style_filename(fn, self.stream)
        self.stream.seek(0)
        new_fn = srl.deserialize_old_style_filename(self.stream)
        self.assertEqual(fn, new_fn)


def _compile_java_part(java_class_file, classpath):
    java_file = os.path.splitext(
        os.path.realpath(java_class_file)
    )[0] + '.java'
    if (not os.path.exists(java_class_file) or
            os.path.getmtime(java_file) > os.path.getmtime(java_class_file)):
        cmd = ['javac', '-cp', classpath, java_file]
        try:
            subprocess.check_call(cmd, cwd=os.path.dirname(java_file))
        except subprocess.CalledProcessError:
            raise RuntimeError("Error compiling Java file %s" % java_file)


def _get_java_output_stream(wd):
    this_directory = os.path.abspath(os.path.dirname(__file__))
    src = os.path.join(this_directory, "%s.java" % _HADOOP_SERIALIZE_CLASS)
    shutil.copy(src, wd)
    classpath = '.:%s:%s' % (pydoop.hadoop_classpath(), wd)
    filename_root = os.path.join(wd, _HADOOP_SERIALIZE_CLASS)
    _compile_java_part(filename_root + ".class", classpath)
    output = subprocess.check_output(
        ['java', '-cp', classpath, _HADOOP_SERIALIZE_CLASS],
        cwd=wd,
        stderr=open('/dev/null', 'w')
    )
    stream = StringIO(output)
    return stream


def suite():
    return unittest.TestLoader().loadTestsFromTestCase(TestSerialize)


if __name__ == '__main__':
    _RUNNER = unittest.TextTestRunner(verbosity=2)
    _RUNNER.run((suite()))
