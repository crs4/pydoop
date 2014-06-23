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

from pydoop.pure.api import Mapper, Reducer, Partitioner, Factory
from pydoop.pure.pipes import run_task

from test_text_stream import stream_writer
import itertools as it

stream_1 = [
    ('start', 0),
    ('setJobConf', 'key1', 'value1', 'key2', 'value2'),
    ('setInputTypes', 'key_type', 'value_type'),
    ('runMap', 'input_split', 0, False),
    ('mapItem', 'key1', 'the blue fox jumps on the table'),
    ('mapItem', 'key1', 'a yellow fox turns around'),
    ('mapItem', 'key2', 'a blue yellow fox sits on the table'),
    ('close',),            
    ]

stream_2 = [
    ('start', 0),
    ('setJobConf', 'key1', 'value1', 'key2', 'value2'),
    ('setInputTypes', 'key_type', 'value_type'),
    ('runMap', 'input_split', 0, False),
    ('mapItem', 'key1', 'the blue fox jumps on the table'),
    ('mapItem', 'key1', 'a yellow fox turns around'),
    ('mapItem', 'key2', 'a blue yellow fox sits on the table'),
    ('runReduce', 0, False),
    ('reduceKey', 'key1'),
    ('reduceValue', 'val1'),
    ('reduceValue', 'val2'),
    ('reduceKey', 'key2'),            
    ('reduceValue', 'val3'),
    ('close',),            
    ]



class TMapper(Mapper):
    def __init__(self, ctx):
        self.ctx = ctx
    def map(self, ctx):
        words = ctx.value.split()
        for w in words:
            ctx.emit(w, '1')
class TReducer(Reducer):
    def __init__(self, ctx):
        self.ctx = ctx
    def reduce(self, ctx):
        s = sum(it.imap(int, ctx.values))
        ctx.emit(ctx.key, str(s))

class TFactory(Factory):
    def __init__(self):
        self.mclass = TMapper
        self.rclass = TReducer
    def create_mapper(self, context):
        return self.mclass(context)
    def create_reducer(self, context):
        return self.rclass(contect)

class TestFramework(unittest.TestCase):

    def setUp(self):
        fname = 'foo.txt'
        stream_writer(fname, stream_1)
        self.stream = open(fname, 'r')

    def test_map_only(self):
        factory = TFactory()
        run_task(factory, istream=self.stream)

def suite():
  suite = unittest.TestSuite()
  suite.addTest(TestFramework('test_map_only'))
  return suite

if __name__ == '__main__':
  runner = unittest.TextTestRunner(verbosity=2)
  runner.run((suite()))
