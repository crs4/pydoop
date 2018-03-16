# BEGIN_COPYRIGHT
#
# Copyright 2009-2018 CRS4.
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
import os
from collections import Counter
import logging

from pydoop.mapreduce.api import Mapper, Reducer, Factory, JobConf
from pydoop.mapreduce.simulator import HadoopSimulatorLocal
from pydoop.mapreduce.simulator import TrivialRecordReader
from pydoop.test_utils import WDTestCase
from pydoop.utils.conversion_tables import mrv1_to_mrv2, mrv2_to_mrv1
from pydoop.utils.py3compat import iteritems, cmap


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
        v = ctx.value
        v = v if not isinstance(v, bytes) else v.decode('utf-8')
        words = ''.join(c for c in v
                        if c.isalnum() or c == ' ').lower().split()
        for w in words:
            ctx.emit(w, '1')


class TReducer(Reducer):

    def __init__(self, ctx):
        super(TReducer, self).__init__(ctx)
        self.ctx = ctx

    def reduce(self, ctx):
        s = sum(cmap(int, ctx.values))
        ctx.emit(ctx.key, str(s))


class TReducerWithCounters(Reducer):

    def __init__(self, ctx):
        super(TReducerWithCounters, self).__init__(ctx)
        self.ctx = ctx
        ctx.get_counter("p", "n")
        self.counters = {}
        for n in COUNTS.keys():
            self.counters[n] = self.ctx.get_counter("DEFAULT", n)

    def reduce(self, ctx):
        s = sum(cmap(int, ctx.values))
        ctx.emit(ctx.key, str(s))
        counter = self.counters[ctx.key]
        ctx.increment_counter(counter, s)


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

    def test_job_conf(self):
        job_conf = {}
        for k in mrv1_to_mrv2:
            job_conf[k] = k
        jc = JobConf(
            [item for sublist in iteritems(job_conf) for item in sublist]
        )
        for k in mrv2_to_mrv1:
            self.assertEqual(jc[k], job_conf[mrv2_to_mrv1[k]])

    def test_job_conf_getters(self):
        values = ['int', '1', 'float', '2.3', 'bool', 'false']
        conv_values = [1, 2.3, False]
        jc = JobConf(values)
        print(jc)
        return
        for i, k in enumerate(values[::2]):
            getter = getattr(jc, 'get_%s' % k)
            self.assertEqual(getter(k), conv_values[i])
        for jc in JobConf([]), JobConf(['x', 'foo']):
            for d in False, True:
                self.assertEqual(jc.get_bool('x', default=d), d)
        self.assertRaises(RuntimeError, JobConf(['x', 'foo']).get_bool, 'x')

    def test_map_only(self):
        job_conf = {'this.is.not.used': '22'}
        hs = HadoopSimulatorLocal(TFactory(), loglevel=logging.CRITICAL)
        with open(self.fname, 'r') as fin:
            with self._mkf('map_only.out', 'w') as fout:
                hs.run(fin, fout, job_conf, 0)
                self.assertTrue(os.stat(fout.name).st_size > 0)

    def test_record_reader(self):
        job_conf = {'this.is.not.used': '22'}
        hs = HadoopSimulatorLocal(TFactory(record_reader=TrivialRecordReader))
        foname = 'map_reduce.out'
        with self._mkf(foname) as fout:
            hs.run(None, fout, job_conf, 0)
            self.assertTrue(os.stat(fout.name).st_size > 0)

    def test_map_reduce(self):
        job_conf = {'this.is.not.used': '22'}
        hs = HadoopSimulatorLocal(TFactory(), loglevel=logging.CRITICAL)
        foname = 'map_reduce.out'
        with open(self.fname, 'r') as fin:
            with self._mkf(foname) as fout:
                hs.run(fin, fout, job_conf, 1)
                self.assertTrue(os.stat(fout.name).st_size > 0)
        with open(self._mkfn(foname)) as f:
            for l in f:
                k, c = l.strip().split()
                self.assertEqual(COUNTS[k], int(c))

    def test_map_reduce_with_counters(self):
        job_conf = {'this.is.not.used': '22'}
        hs = HadoopSimulatorLocal(TFactory(reducer_class=TReducerWithCounters),
                                  loglevel=logging.CRITICAL)
        foname = 'map_reduce.out'
        with open(self.fname, 'r') as fin:
            with self._mkf(foname) as fout:
                hs.run(fin, fout, job_conf, 1)
                self.assertTrue(os.stat(fout.name).st_size > 0)
        with open(self._mkfn(foname)) as f:
            for l in f:
                k, c = l.strip().split()
                if "COUNTER_" in k:
                    ck = int(k[8:]) - 1
                    key = COUNTS.keys()[ck]
                    self.assertEqual(COUNTS[key], int(c))
                else:
                    self.assertEqual(COUNTS[k], int(c))

    def test_map_combiner_reduce(self):
        job_conf = {'this.is.not.used': '22'}
        hs = HadoopSimulatorLocal(TFactory(combiner=TReducer))
        foname = 'map_combiner_reduce.out'
        with open(self.fname, 'r') as fin:
            with self._mkf(foname) as fout:
                hs.run(fin, fout, job_conf, 1)
                self.assertTrue(os.stat(fout.name).st_size > 0)
        with open(self._mkfn(foname)) as f:
            for l in f:
                k, c = l.strip().split()
                self.assertEqual(COUNTS[k], int(c))


def suite():
    suite_ = unittest.TestSuite()
    suite_.addTest(TestFramework('test_job_conf'))
    # suite_.addTest(TestFramework('test_job_conf_getters'))
    # suite_.addTest(TestFramework('test_map_only'))
    # suite_.addTest(TestFramework('test_map_reduce'))
    # suite_.addTest(TestFramework('test_map_combiner_reduce'))
    # suite_.addTest(TestFramework('test_record_reader'))
    # suite_.addTest(TestFramework('test_map_reduce_with_counters'))
    return suite_


if __name__ == '__main__':
    _RUNNER = unittest.TextTestRunner(verbosity=2)
    _RUNNER.run((suite()))
