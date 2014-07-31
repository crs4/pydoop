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

import unittest, cStringIO, random

#FIXME
import os
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../'))

import pydoop.mapreduce.serialize as srl
import pydoop.mapreduce.jwritable_utils as wu
from serialize.test_serialize import _get_java_output_stream

class TestSerialize(unittest.TestCase):

    def setUp(self):
        self.stream = cStringIO.StringIO()

    def test_array(self, array):
        stream = self.stream
        stream.seek(0)        
        for a in array:
            self.serialize(a, stream)
        stream.seek(0)
        for a in array:
            self.assertEqual(a, self.deserialize(stream))
    def test_vint(self):
        self.serialize = srl.serialize_vint
        self.deserialize = srl.deserialize_vint
        self.test_array(range(-16782,16782))
        self.test_array(random.sample(xrange(-18999289888, 18999289888), 10000))

    def test_int(self):
        self.serialize = srl.serialize_int
        self.deserialize = srl.deserialize_int
        self.test_array(range(-16782,16782))

    def test_float(self):
        stream = self.stream
        numbers  = [random.uniform(-100000, 100000) for _ in range(10000)]
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
        self.serialize = srl.serialize_string
        self.deserialize = srl.deserialize_string
        self.test_array(open(__file__).readlines())

    def test_string_compressed(self):
        self.serialize = srl.serialize_string_compressed
        self.deserialize = srl.deserialize_string_compressed
        self.test_array(open(__file__).readlines())

    def test_generic(self):
        stream = self.stream
        vals = [1, False, 0.3290, 1902, 'sshjdhsj', 0.3, -33, 'ueiwriuqrei']
        for v in vals:
            srl.serialize(v, stream)
        stream.seek(0)
        for v in vals:
            x = srl.deserialize(type(v), stream)
            if isinstance(v, float):
                self.assertTrue(abs(v-x)/abs(v+x) < 1e-6)
            else:
                self.assertEqual(v, x)

    def test_hadoop_base_types(self):
        srl.register_deserializer('WUString', wu.readString)

        values = [('org.apache.hadoop.io.VIntWritable', 42),
                  ('org.apache.hadoop.io.VIntWritable', 4242),
                  ('org.apache.hadoop.io.VIntWritable', 424242),
                  ('org.apache.hadoop.io.VIntWritable', 42424242),
                  ('org.apache.hadoop.io.VIntWritable', -42),
                  ('org.apache.hadoop.io.VLongWritable', 42),
                  ('org.apache.hadoop.io.VLongWritable', 424242),
                  ('org.apache.hadoop.io.VLongWritable', 4242424242),
                  ('WUString', "hello world"),
                  ('WUString', u"oggi \u00e8 gioved\u00ec"),
                  ('org.apache.hadoop.io.Text', u"\u00e0 Text object"),
                  ]
        java_stream = _get_java_output_stream()
        for t, v in values:
            x = srl.deserialize(t, java_stream)
            self.assertEqual(v, x)

def suite():
    suite = unittest.TestSuite()
    suite.addTest(TestSerialize('test_int'))
    suite.addTest(TestSerialize('test_vint'))
    suite.addTest(TestSerialize('test_float'))
    suite.addTest(TestSerialize('test_string'))
    suite.addTest(TestSerialize('test_string_compressed'))    
    suite.addTest(TestSerialize('test_generic'))
    suite.addTest(TestSerialize('test_hadoop_base_types'))    
    return suite


if __name__ == '__main__':
    runner = unittest.TextTestRunner(verbosity=2)
    runner.run((suite()))
