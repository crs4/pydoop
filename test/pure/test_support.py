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
from pydoop.pure.pipes import PipesRunner
import itertools as it

data = \
"""1	Chapter One  Down the Rabbit Hole: Alice is feeling bored while
1	sitting on the riverbank with her elder sister, when she notices a
1	talking, clothed White Rabbit with a pocket watch run past. She
1	follows it down a rabbit hole when suddenly she falls a long way to a
1	curious hall with many locked doors of all sizes. She finds a small
1	key to a door too small for her to fit through, but through it she
1	sees an attractive garden. She then discovers a bottle on a table
1	labelled "DRINK ME," the contents of which cause her to shrink too
1	small to reach the key which she has left on the table. She eats a
1	cake with "EAT ME" written on it in currants as the chapter closes.
"""

class TMapper(Mapper):
    def __init__(self, ctx):
        self.ctx = ctx
    def map(self, ctx):
        words = ''.join(c for c in ctx.value
                        if c.isalnum() or c == ' ').lower().split()
        for w in words:
            ctx.emit(w, '1')
class TReducer(Reducer):
    def __init__(self, ctx):
        self.ctx = ctx
    def reduce(self, ctx):
        s = sum(it.imap(int, ctx.values))
        ctx.emit(ctx.key, str(s))

class TFactory(Factory):
    def __init__(self, combiner=None, partitioner=None,
                 record_writer=None, record_reader=None):
        self.mclass = TMapper
        self.rclass = TReducer
        self.cclass = combiner
        self.pclass = partitioner
        self.rwclass = record_writer
        self.rrclass = record_reader
    def create_mapper(self, context):
        return self.mclass(context)
    def create_reducer(self, context):
        return self.rclass(context)
    def create_combiner(self, context):
        return None if not self.cclass else self.cclass(context)
    def create_partitioner(self, context):
        return None if not self.pclass else self.pclass(context)
    def create_record_reader(self, context):
        return None if not self.rrclass else self.rrclass(context)
    def create_record_writer(self, context):
        return None if not self.rwclass else self.rwclass(context)
        
class TestFramework(unittest.TestCase):

    def setUp(self):
        self.fname = 'alice.txt'
        with open(self.fname, 'w') as f:
            f.write(data)

    def test_map_only(self):
        job_conf = {'this.is.not.used' : '22'}
        pr = PipesRunner(TFactory())
        with open(self.fname, 'r') as fin, open('map_only.out', 'w') as fout:
            pr.run(fin, fout, job_conf, 0)

    def test_map_reduce(self):
        job_conf = {'this.is.not.used' : '22'}
        pr = PipesRunner(TFactory())
        with open(self.fname, 'r') as fin, open('map_reduce.out', 'w') as fout:
            pr.run(fin, fout, job_conf, 1)

    def test_map_combiner_reduce(self):
        job_conf = {'this.is.not.used' : '22'}
        pr = PipesRunner(TFactory(combiner=TReducer))
        with open(self.fname, 'r') as fin, open('map_combiner_reduce.out', 'w') as fout:
            pr.run(fin, fout, job_conf, 1)

def suite():
  suite = unittest.TestSuite()
  suite.addTest(TestFramework('test_map_only'))
  suite.addTest(TestFramework('test_map_reduce'))
  suite.addTest(TestFramework('test_map_combiner_reduce'))    
  return suite

if __name__ == '__main__':
  runner = unittest.TextTestRunner(verbosity=2)
  runner.run((suite()))
