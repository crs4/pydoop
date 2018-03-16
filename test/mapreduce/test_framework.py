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

import unittest
from collections import Counter
import time

from pydoop.mapreduce.api import Mapper, Reducer, Factory
from pydoop.mapreduce.pipes import run_task, TaskContext

from pydoop.test_utils import WDTestCase
from pydoop.utils.misc import Timer
from pydoop.utils.py3compat import iteritems


from test_cmd_streams import stream_writer
from pydoop.mapreduce.streams import StreamWriter
from pydoop.mapreduce.text_streams import TextWriter
from pydoop.mapreduce.binary_streams import BinaryWriter
from pydoop.mapreduce.binary_streams import BinaryDownStreamAdapter
from pydoop.mapreduce.binary_streams import BinaryUpStreamDecoder

from data.stream_data import STREAM_5_DATA as STREAM_1
from data.stream_data import STREAM_6_DATA as STREAM_2


def binary_stream_writer(fname, data):
    stream_writer(fname, data, 'b', BinaryWriter)


def text_stream_writer(fname, data):
    stream_writer(fname, data, '', TextWriter)


def count_outputs(fname):
    with open(fname) as f:
        count = Counter([_.strip().split('\t', 1)[0] for _ in f])
    return count


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
            ctx.emit(w, 1)


class TReducer(Reducer):

    def __init__(self, ctx):
        self.ctx = ctx

    def reduce(self, ctx):
        s = sum(map(int, ctx.values))
        ctx.emit(ctx.key, s)


class SleepingMapper(TMapper):

    def __init__(self, ctx):
        super(SleepingMapper, self).__init__(ctx)
        self.timer = Timer(ctx)

    def map(self, ctx):
        with self.timer.time_block("sleep"):
            time.sleep(0.001)
        super(SleepingMapper, self).map(ctx)


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
    """
    This class simulates hadoop sort and shuffle. It uses the
    TextStream protocol.
    """

    def __init__(self):
        self.data = {}
        self.buffer = []
        self.out_stream = None

    def process(self, msg):
        p = msg.split(TextWriter.SEP)
        if p[0] == TextWriter.CMD_TABLE[StreamWriter.OUTPUT]:
            key = p[1]
            val = p[2]
            self.data.setdefault(key, []).append(val)

    def write(self, s):
        self.buffer.append(s)
        if s.endswith(TextWriter.EOL):
            msg = ''.join(self.buffer)
            self.buffer = []
            self.process(msg)

    def flush(self):
        pass

    def close(self):
        self.out_stream = self.get_reduce_stream()

    def readline(self):
        try:
            return next(self.out_stream)
        except StopIteration:
            return ''

    def get_reduce_stream(self):
        yield 'runReduce\t0\t0\n'
        for k in self.data:
            yield 'reduceKey\t{}\n'.format(k)
            for v in self.data[k]:
                yield 'reduceValue\t{}\n'.format(v)
        yield 'close\n'


class TestFramework(WDTestCase):

    def setUp(self):
        super(TestFramework, self).setUp()
        fname = self._mkfn('foo.txt')
        text_stream_writer(fname, STREAM_1)
        self.stream1 = open(fname, 'r')
        fname = self._mkfn('foo2.txt')
        text_stream_writer(fname, STREAM_2)
        self.stream2 = open(fname, 'r')
        fname = self._mkfn('foo3.bin')
        binary_stream_writer(fname, STREAM_2)
        self.stream3 = open(fname, 'rb')

    def tearDown(self):
        self.stream1.close()
        self.stream2.close()
        self.stream3.close()
        super(TestFramework, self).tearDown()

    def test_map_only(self):
        factory = TFactory()
        fname = self._mkfn('foo_map_only.out')
        with open(fname, 'w') as o:
            run_task(factory, istream=self.stream1, ostream=o)
        exp_count = {
            'done': 1,
            'progress': 1,
            'output': sum(len(_[2].split())
                          for _ in STREAM_1 if _[0] is TextWriter.MAP_ITEM)
        }
        self.check_counts(fname, exp_count)

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
        bw = BinaryWriter
        with open(out_file, 'rb') as f:
            bf = BinaryDownStreamAdapter(f)
            for cmd, args in bf:
                if cmd == bw.OUTPUT:
                    data.setdefault(args[0], []).append(args[1])
        stream = []
        stream.append((bw.START_MESSAGE, 0))
        stream.append((bw.SET_JOB_CONF, 'key1', 'value1', 'key2', 'value2'))
        stream.append((bw.RUN_REDUCE, 0, 0))
        for k in data:
            stream.append((bw.REDUCE_KEY, k))
            for v in data[k]:
                stream.append((bw.REDUCE_VALUE, v))
        stream.append((bw.CLOSE,))
        binary_stream_writer(reduce_infile, stream)
        run_task(factory, cmd_file=reduce_infile, private_encoding=True)
        with open(reduce_outfile, 'rb') as f:
            with self._mkf('foo.out', mode='w') as o:
                bf = BinaryUpStreamDecoder(f)
                for cmd, args in bf:
                    if cmd == bw.PROGRESS:
                        o.write('progress\t%s\n' % args[0])
                    elif cmd == bw.OUTPUT:
                        o.write('output\t%s\n' %
                                '\t'.join([x.decode('utf-8') for x in args]))
                    elif cmd == bw.DONE:
                        o.write('done\n')
        self.check_result('foo.out', STREAM_2)

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

    def test_timer(self):
        factory = TFactory(mapper=SleepingMapper)
        exp_count = {
            'registerCounter': 1,
            'incrementCounter': Counter(
                [_[0] for _ in STREAM_1]
            )[TextWriter.MAP_ITEM]
        }
        with self._mkf('foo_map_only.out') as o:
            run_task(factory, istream=self.stream1, ostream=o)
            self.check_counts(o.name, exp_count)

    def check_counts(self, fname, exp_count):
        count = count_outputs(fname)
        try:
            for k, v in iteritems(exp_count):
                self.assertTrue(k in count)
                self.assertEqual(count[k], v)
        except AssertionError:
            print(count)
            raise

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
        ref_counts = Counter(
            sum([x[2].split()
                 for x in ref_data if x[0] == StreamWriter.MAP_ITEM], []
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
    return suite_


if __name__ == '__main__':
    _RUNNER = unittest.TextTestRunner(verbosity=2)
    _RUNNER.run((suite()))
