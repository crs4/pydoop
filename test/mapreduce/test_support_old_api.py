# BEGIN_COPYRIGHT
#
# Copyright 2009-2015 CRS4.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy
# of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.
#
# END_COPYRIGHT

import unittest
import itertools as it
from collections import Counter

from pydoop.pipes import Mapper, Reducer, Factory
from pydoop.mapreduce.simulator import HadoopSimulatorLocal
from pydoop.mapreduce.simulator import TrivialRecordReader
from pydoop.test_utils import WDTestCase


DATA = \
    """Chapter One  Down the Rabbit Hole: Alice is feeling bored while
    sitting on the riverbank with her elder sister, when she notices a
    talking, clothed White Rabbit with a pocket watch run past. She
    follows it down a rabbit hole when suddenly she falls a long way to a
    curious hall with many locked doors of all sizes. She finds a small
    key to a door too small for her to fit through, but through it she
    sees an attractive garden. She then discovers a bottle on a table
    labelled "DRINK ME," the contents of which cause her to shrink too
    small to reach the key which she has left on the table. She eats a
    cake with "EAT ME" written on it in currants as the chapter closes."""

COUNTS = Counter(''.join(c for c in DATA.replace('1\t', ' ')
                         if c.isalnum() or c == ' ').lower().split())


class TMapper(Mapper):

    def __init__(self, ctx):
        super(TMapper, self).__init__(ctx)
        self.ctx = ctx

    def map(self, ctx):
        words = ''.join(c for c in ctx.getInputValue()
                        if c.isalnum() or c == ' ').lower().split()
        for w in words:
            ctx.emit(w, '1')


class TReducer(Reducer):

    def __init__(self, ctx):
        super(TReducer, self).__init__(ctx)
        self.ctx = ctx

    def reduce(self, ctx):
        s = sum(it.imap(int, ctx.getInputValues()))
        ctx.emit(ctx.getInputKey(), str(s))


class TReducerWithCounters(Reducer):

    def __init__(self, ctx):
        super(TReducerWithCounters, self).__init__(ctx)
        self.ctx = ctx
        self.counters = {}
        self.output_words = self.ctx.getCounter("DEFAULT", "OUTPUTWORD")

    def reduce(self, ctx):
        s = 0
        while ctx.nextValue():
            s += int(ctx.getInputValue())
        ctx.emit(ctx.getInputKey(), str(s))
        ctx.incrementCounter(self.output_words, s)


class TFactory(Factory):

    def __init__(self, combiner=None, partitioner=None, reducer_class=TReducer,
                 record_writer=None, record_reader=None):
        self.mclass = TMapper
        self.rclass = reducer_class
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


class TestFramework(WDTestCase):
    def setUp(self):
        super(TestFramework, self).setUp()
        self.fname = self._mkfn('alice.txt')
        with open(self.fname, 'w') as fo:
            fo.write(DATA)

    def test_map_only(self):
        job_conf = {'this.is.not.used': '22'}
        hs = HadoopSimulatorLocal(TFactory())
        with open(self.fname, 'r') as fin:
            with self._mkf('map_only.out') as fout:
                hs.run(fin, fout, job_conf, 0)

    def test_record_reader(self):
        job_conf = {'this.is.not.used': '22'}
        hs = HadoopSimulatorLocal(TFactory(record_reader=TrivialRecordReader))
        foname = 'map_reduce.out'
        with self._mkf(foname) as fout:
            hs.run(None, fout, job_conf, 0)

    def test_map_reduce(self):
        job_conf = {'this.is.not.used': '22'}
        hs = HadoopSimulatorLocal(TFactory())
        foname = 'map_reduce.out'
        with open(self.fname, 'r') as fin:
            with self._mkf(foname) as fout:
                hs.run(fin, fout, job_conf, 1)
        with open(self._mkfn(foname)) as f:
            for l in f:
                k, c = l.strip().split()
                self.assertEqual(COUNTS[k], int(c))

    def test_map_reduce_with_counters(self):
        job_conf = {'this.is.not.used': '22'}
        hs = HadoopSimulatorLocal(TFactory(reducer_class=TReducerWithCounters))
        foname = 'map_reduce.out'

        with open(self.fname, 'r') as fin:
            with self._mkf(foname) as fout:
                hs.run(fin, fout, job_conf, 1)

        with open(self._mkfn(foname)) as f:
            sum_ = 0
            counter_value = 0
            for l in f:
                k, c = l.strip().split()
                if "COUNTER_" in k:
                    counter_value = int(c)
                    self.assertEqual(sum_, counter_value)
                else:
                    sum_ += int(c)
                    self.assertEqual(COUNTS[k], int(c))

    def test_map_combiner_reduce(self):
        job_conf = {'this.is.not.used': '22'}
        hs = HadoopSimulatorLocal(TFactory(combiner=TReducer))
        foname = 'map_combiner_reduce.out'
        with open(self.fname, 'r') as fin:
            with self._mkf(foname) as fout:
                hs.run(fin, fout, job_conf, 1)
        with open(self._mkfn(foname)) as f:
            for l in f:
                k, c = l.strip().split()
                self.assertEqual(COUNTS[k], int(c))


def suite():
    suite_ = unittest.TestSuite()
    suite_.addTest(TestFramework('test_map_only'))
    suite_.addTest(TestFramework('test_map_reduce'))
    suite_.addTest(TestFramework('test_map_combiner_reduce'))
    suite_.addTest(TestFramework('test_record_reader'))
    suite_.addTest(TestFramework('test_map_reduce_with_counters'))
    return suite_


if __name__ == '__main__':
    _RUNNER = unittest.TextTestRunner(verbosity=2)
    _RUNNER.run((suite()))
