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

import unittest

#FIXME
import sys
sys.path.insert(0, '../../')

import pydoop.pure.serialize as srl

import cStringIO
import random

class TestSerialize(unittest.TestCase):
    def setUp(self):
        self.stream = cStringIO.StringIO()                
    
    def test_int(self):
        stream = self.stream
        for i in range(-16782,16782):
            srl.serialize_int(i, stream)
        stream.seek(0)
        for i in range(-16782,16782):
            x = srl.deserialize_int(stream)
            self.assertEqual(i, x)

    def test_int_big(self):
        stream = self.stream
        numbers = random.sample(xrange(-18999289888, 18999289888), 10000)
        for i in numbers:
            srl.serialize_int(i, stream)
        stream.seek(0)
        for i in numbers:
            x = srl.deserialize_int(stream)
            self.assertEqual(i, x)

    def test_float(self):
        stream = self.stream
        numbers = [random.uniform(-100000, 100000)
                   for _ in range(10000)]
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
            s = f.read()
        t = s
        for _ in range(N):
            t = t[::-1]
            srl.serialize_string(t, stream)
        stream.seek(0)
        t = s        
        for _ in range(N):
            t = t[::-1]
            s1 = srl.deserialize_string(stream)
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
                srl.serialize_string(v, stream)
        stream.seek(0)
        for v in vals:
            if isinstance(v, int):
                x = srl.deserialize_int(stream)
                self.assertEqual(v, x)
            elif isinstance(v, float):
                x = srl.deserialize_float(stream)
                self.assertTrue(abs(v-x)/abs(v+x) < 1e-6)
            elif isinstance(v, str):
                x = srl.deserialize_string(stream)
                self.assertEqual(v, x)
    

def suite():
  suite = unittest.TestSuite()
  suite.addTest(TestSerialize('test_int'))
  suite.addTest(TestSerialize('test_int_big'))
  suite.addTest(TestSerialize('test_float'))
  suite.addTest(TestSerialize('test_string'))
  suite.addTest(TestSerialize('test_mixture'))  
  return suite

if __name__ == '__main__':
  runner = unittest.TextTestRunner(verbosity=2)
  runner.run((suite()))
