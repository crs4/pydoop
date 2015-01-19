# BEGIN_COPYRIGHT
#
# Copyright 2009-2015 CRS4.
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
from collections import Counter

from pydoop.mapreduce.api import Mapper, Reducer, Factory
from pydoop.mapreduce.pipes import run_task, TaskContext
from pydoop.mapreduce.string_utils import unquote_string
from pydoop.test_utils import WDTestCase

from test_text_stream import stream_writer


STREAM_1 = [
    ('start', 0),
    ('setJobConf', 'key1', 'value1', 'key2', 'value2'),
    ('setInputTypes', 'key_type', 'value_type'),
    ('runMap', 'input_split', 0, False),
    ('mapItem', 'key1', 'the blue fox jumps on the table'),
    ('mapItem', 'key1', 'a yellow fox turns around'),
    ('mapItem', 'key2', 'a blue yellow fox sits on the table'),
    ('close',),
    ]


class TContext(TaskContext):

    @property
    def value(self):
        return ' '.join([self.get_input_value()] * 2)


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
        s = sum(map(int, ctx.values))
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


class SortAndShuffle(object):

    def __init__(self):
        self.data = {}
        self.buffer = []
        self.out_stream = None

    def process(self, msg):
        parts = msg.split('\t')
        if parts[0] == 'output':
            key = unquote_string(parts[1])
            value = unquote_string(parts[2])
            self.data.setdefault(key, []).append(value)

    def write(self, s):
        self.buffer.append(s)
        if s.endswith('\n'):
            msg = ''.join(self.buffer)
            self.buffer = []
            self.process(msg)

    def flush(self):
        pass

    def close(self):
        self.out_stream = self.get_reduce_stream()

    def readline(self):
        try:
            return self.out_stream.next()
        except StopIteration:
            return ''

    def get_reduce_stream(self):
        yield 'runReduce\t0\t0\n'
        for k in self.data:
            yield 'reduceKey\t%s\n' % k
            for v in self.data[k]:
                yield 'reduceValue\t%s\n' % v
        yield 'close\n'


class TestFramework(WDTestCase):

    def setUp(self):
        super(TestFramework, self).setUp()
        fname = self._mkfn('foo.txt')
        stream_writer(fname, STREAM_1)
        self.stream = open(fname, 'r')

    def tearDown(self):
        self.stream.close()
        super(TestFramework, self).tearDown()

    def test_map_only(self):
        factory = TFactory()
        with self._mkf('foo_map_only.out') as o:
            run_task(factory, istream=self.stream, ostream=o)

    def test_map_reduce(self):
        factory = TFactory()
        sas = SortAndShuffle()
        run_task(factory, istream=self.stream, ostream=sas,
                 private_encoding=False)
        with self._mkf('foo_map_reduce.out') as o:
            run_task(factory, istream=sas, ostream=o,
                     private_encoding=False)
        self.check_result('foo_map_reduce.out', STREAM_1)

    def test_map_combiner_reduce(self):
        factory = TFactory(combiner=TReducer)
        sas = SortAndShuffle()
        run_task(factory, istream=self.stream, ostream=sas)
        with self._mkf('foo_map_combiner_reduce.out') as o:
            run_task(factory, istream=sas, ostream=o,
                     private_encoding=False)
        self.check_result('foo_map_combiner_reduce.out', STREAM_1)

    def test_map_combiner_reduce_with_context(self):
        factory = TFactory(combiner=TReducer)
        sas = SortAndShuffle()
        run_task(
            factory, istream=self.stream, ostream=sas, context_class=TContext
        )
        with self._mkf('foo_map_combiner_reduce.out') as o:
            run_task(factory, istream=sas, ostream=o,
                     private_encoding=False)
        self.check_result('foo_map_combiner_reduce.out', STREAM_1, 2)

    def check_result(self, fname, ref_data, factor=1):
        with self._mkf(fname, mode='r') as i:
            data = i.read()
        lines = data.strip().split('\n')
        self.assertEqual('progress\t0.0', lines[0])
        self.assertEqual('done', lines[-1])
        counts = Counter(dict(map(lambda t: (t[0], int(t[1])),
                                  (l.split('\t')[1:] for l in lines[1:-1]))))
        #--
        ref_counts = Counter(sum(
            [x[2].split() for x in ref_data if x[0] == 'mapItem'], []
        ))
        self.assertEqual(counts.keys(), ref_counts.keys())
        for k in counts.keys():
            self.assertEqual(ref_counts[k] * factor, counts[k])


def suite():
    suite_ = unittest.TestSuite()
    suite_.addTest(TestFramework('test_map_only'))
    suite_.addTest(TestFramework('test_map_reduce'))
    suite_.addTest(TestFramework('test_map_combiner_reduce'))
    suite_.addTest(TestFramework('test_map_combiner_reduce_with_context'))
    return suite_


if __name__ == '__main__':
    _RUNNER = unittest.TextTestRunner(verbosity=2)
    _RUNNER.run((suite()))
