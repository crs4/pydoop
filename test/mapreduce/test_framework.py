# BEGIN_COPYRIGHT
#
# Copyright 2009-2016 CRS4.
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
import time

from pydoop.mapreduce.api import Mapper, Reducer, Factory
from pydoop.mapreduce.pipes import run_task, TaskContext
from pydoop.mapreduce.string_utils import unquote_string
from pydoop.test_utils import WDTestCase
from pydoop.utils.misc import Timer

from test_text_stream import stream_writer
from test_binary_streams import stream_writer as binary_stream_writer
from pydoop.mapreduce.binary_streams import BinaryDownStreamFilter
from pydoop.mapreduce.binary_streams import BinaryUpStreamDecoder


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

STREAM_2 = [
    ('start', 0),
    ('setJobConf', 'key1', 'value1', 'key2', 'value2'),
    ('setInputTypes', 'key_type', 'value_type'),
    ('runMap', 'input_split', 1, False),
    ('mapItem', 'key1', 'the blue fox jumps on the table'),
    ('mapItem', 'key1', 'a yellow fox turns around'),
    ('mapItem', 'key2', 'a blue yellow fox sits on the table'),
    ('close',),
]

STREAM_3 = [
    ('start', 0),
    ('setJobConf', ('key1', 'value1', 'key2', 'value2')),
    ('setInputTypes', 'key_type', 'value_type'),
    ('runMap', 'input_split', 1, False),
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


class SleepingMapper(TMapper):

    def __init__(self, ctx):
        super(SleepingMapper, self).__init__(ctx)
        self.timer = Timer(ctx)

    def map(self, ctx):
        with self.timer.time_block("sleep"):
            time.sleep(.001)
        super(SleepingMapper, self).map(ctx)


class SleepingMapperNoTimer(TMapper):

    def __init__(self, ctx):
        super(SleepingMapperNoTimer, self).__init__(ctx)

    def map(self, ctx):
        time.sleep(.001)
        super(SleepingMapperNoTimer, self).map(ctx)


class TMapperPE(Mapper):

    def __init__(self, ctx):
        self.ctx = ctx

    def map(self, ctx):
        words = ctx.value.split()
        ctx.emit('', Counter(words))


class TReducerPE(Reducer):

    def __init__(self, ctx):
        self.ctx = ctx

    def reduce(self, ctx):
        s = sum(ctx.values, Counter())
        for k in s:
            ctx.emit(k, s[k])


class TCombinerPE(Reducer):

    def __init__(self, ctx):
        self.ctx = ctx

    def reduce(self, ctx):
        s = sum(ctx.values, Counter())
        ctx.emit('', s)


class TMapperSE(Mapper):

    def __init__(self, ctx):
        self.ctx = ctx
        self.counter = Counter()

    def map(self, ctx):
        self.counter.clear()
        self.counter.update(ctx.value.split())
        ctx.emit('', self.counter)


class TReducerSE(Reducer):

    def __init__(self, ctx):
        self.ctx = ctx

    def reduce(self, ctx):
        s = sum(ctx.values, Counter())
        for k in s:
            ctx.emit(k, s[k])


class TCombinerSE(Reducer):

    def __init__(self, ctx):
        self.ctx = ctx

    def reduce(self, ctx):
        s = sum(ctx.values, Counter())
        ctx.emit('', s)


class TFactory(Factory):

    def __init__(self, mapper=TMapper, reducer=TReducer,
                 combiner=None, partitioner=None,
                 record_writer=None, record_reader=None):
        self.mclass = mapper
        self.rclass = reducer
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
        p = msg.split('\t')
        if p[0] == 'output':
            key = unquote_string(p[1])
            val = unquote_string(p[2])
            self.data.setdefault(key, []).append(val)

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
        self.stream1 = open(fname, 'r')
        fname = self._mkfn('foo2.txt')
        stream_writer(fname, STREAM_2)
        self.stream2 = open(fname, 'r')
        fname = self._mkfn('foo3.bin')
        binary_stream_writer(fname, STREAM_3)
        self.stream3 = open(fname, 'r')

    def tearDown(self):
        self.stream1.close()
        self.stream2.close()
        super(TestFramework, self).tearDown()

    def test_map_only(self):
        factory = TFactory()
        with self._mkf('foo_map_only.out') as o:
            run_task(factory, istream=self.stream1, ostream=o)

    def test_map_reduce(self):
        factory = TFactory()
        sas = SortAndShuffle()
        run_task(factory, istream=self.stream1, ostream=sas,
                 private_encoding=False)
        with self._mkf('foo_map_reduce.out') as o:
            run_task(factory, istream=sas, ostream=o,
                     private_encoding=False)
        self.check_result('foo_map_reduce.out', STREAM_1)

    def test_map_combiner_reduce(self):
        factory = TFactory(combiner=TReducer)
        sas = SortAndShuffle()
        run_task(factory, istream=self.stream2, ostream=sas,
                 private_encoding=False)
        with self._mkf('foo_map_combiner_reduce.out') as o:
            run_task(factory, istream=sas, ostream=o,
                     private_encoding=False)
        self.check_result('foo_map_combiner_reduce.out', STREAM_2)

    def test_map_reduce_comb_with_private_encoding(self):
        factory = TFactory(mapper=TMapperPE, combiner=TCombinerPE,
                           reducer=TReducerPE)
        self._test_map_reduce_with_private_encoding_helper(factory,
                                                           fast_combiner=True)

    def test_map_reduce_with_private_encoding(self):
        factory = TFactory(mapper=TMapperPE, reducer=TReducerPE)
        self._test_map_reduce_with_private_encoding_helper(factory)

    def test_map_reduce_comb_with_side_effect(self):
        factory = TFactory(mapper=TMapperSE, combiner=TCombinerSE,
                           reducer=TReducerSE)
        self._test_map_reduce_with_private_encoding_helper(factory,
                                                           fast_combiner=False)

    def _test_map_reduce_with_private_encoding_helper(self, factory,
                                                      fast_combiner=False):
        self.stream3.close()
        cmd_file = self.stream3.name
        out_file = cmd_file + '.out'
        reduce_infile = cmd_file + '.reduce'
        reduce_outfile = reduce_infile + '.out'
        run_task(factory, cmd_file=cmd_file, private_encoding=True,
                 fast_combiner=fast_combiner)
        data = {}
        with open(out_file) as f:
            bf = BinaryDownStreamFilter(f)
            for cmd, args in bf:
                if cmd == 'output':
                    data.setdefault(args[0], []).append(args[1])
        stream = []
        stream.append(('start', 0))
        stream.append(('setJobConf', ('key1', 'value1', 'key2', 'value2')))
        stream.append(('runReduce', 0, False))
        for k in data:
            stream.append(('reduceKey', k))
            for v in data[k]:
                stream.append(('reduceValue', v))
        stream.append(('close',))
        binary_stream_writer(reduce_infile, stream)
        run_task(factory, cmd_file=reduce_infile, private_encoding=True)
        with open(reduce_outfile) as f, self._mkf('foo.out', mode='w') as o:
            bf = BinaryUpStreamDecoder(f)
            for cmd, args in bf:
                if cmd == 'progress':
                    o.write('progress\t%s\n' % args[0])
                elif cmd == 'output':
                    o.write('output\t%s\n' % '\t'.join(args))
                elif cmd == 'done':
                    o.write('done\n')
        self.check_result('foo.out', STREAM_3)

    def test_map_combiner_reduce_with_context(self):
        factory = TFactory(combiner=TReducer)
        sas = SortAndShuffle()
        run_task(
            factory, istream=self.stream2, ostream=sas, context_class=TContext,
            private_encoding=False
        )
        with self._mkf('foo_map_combiner_reduce.out') as o:
            run_task(factory, istream=sas, ostream=o,
                     private_encoding=False)
        self.check_result('foo_map_combiner_reduce.out', STREAM_2, 2)

    def test_instrumentation(self):
        factory = TFactory(mapper=SleepingMapperNoTimer)
        with self._mkf('foo_map_only.out') as o:
            run_task(factory, istream=self.stream1, ostream=o)
        count = Counter()
        with open(o.name) as f:
            for line in f:
                count[line.strip().split('\t', 1)[0]] += 1
        exp_count = {
            'registerCounter': 1,
            'incrementCounter': Counter([_[0] for _ in STREAM_1])['mapItem']
        }
        for k, v in exp_count.iteritems():
            self.assertTrue(k in count)
            self.assertEqual(count[k], v)

    def test_timer(self):
        factory = TFactory(mapper=SleepingMapper)
        with self._mkf('foo_map_only.out') as o:
            run_task(factory, istream=self.stream1, ostream=o)
        count = Counter()
        with open(o.name) as f:
            for line in f:
                count[line.strip().split('\t', 1)[0]] += 1
        exp_count = {
            'registerCounter': 2,
            'incrementCounter': 2 * Counter(
                [_[0] for _ in STREAM_1]
            )['mapItem']
        }
        for k, v in exp_count.iteritems():
            self.assertTrue(k in count)
            self.assertEqual(count[k], v)

    def check_result(self, fname, ref_data, factor=1):
        with self._mkf(fname, mode='r') as i:
            data = i.read()
        lines = data.strip().split('\n')
        self.assertTrue(lines[0] in ['progress\t0.0', 'progress (0.0,)'])
        self.assertTrue(lines[-1] in ['done', 'done ()'])
        counts = Counter(dict(map(lambda t: (t[0], int(t[1])),
                                  (l.split('\t')[1:] for l in lines[1:-1]
                                   if l.startswith('output')
                                   ))))
        ref_counts = Counter(sum(
            [x[2].split() for x in ref_data if x[0] == 'mapItem'], []
        ))
        self.assertEqual(sorted(counts.keys()), sorted(ref_counts.keys()))
        for k in counts.keys():
            self.assertEqual(ref_counts[k] * factor, counts[k])


def suite():
    suite_ = unittest.TestSuite()
    suite_.addTest(TestFramework('test_map_only'))
    suite_.addTest(TestFramework('test_map_reduce'))
    suite_.addTest(TestFramework('test_map_combiner_reduce'))
    suite_.addTest(TestFramework('test_map_combiner_reduce_with_context'))
    suite_.addTest(TestFramework('test_map_reduce_with_private_encoding'))
    suite_.addTest(TestFramework('test_map_reduce_comb_with_private_encoding'))
    suite_.addTest(TestFramework('test_map_reduce_comb_with_side_effect'))
    suite_.addTest(TestFramework('test_timer'))
    suite_.addTest(TestFramework('test_instrumentation'))
    return suite_


if __name__ == '__main__':
    _RUNNER = unittest.TextTestRunner(verbosity=2)
    _RUNNER.run((suite()))
