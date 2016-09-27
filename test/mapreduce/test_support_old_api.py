# BEGIN_COPYRIGHT
#
# Copyright 2009-2016 CRS4.
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
from pydoop.pipes import Mapper, Reducer, Factory
from pydoop.utils.py3compat import cmap
import test_support


class TMapper(Mapper):

    def __init__(self, ctx):
        super(TMapper, self).__init__(ctx)
        self.ctx = ctx

    def map(self, ctx):
        v = ctx.getInputValue()
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
        s = sum(cmap(int, ctx.getInputValues()))
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

test_support.TFactory = TFactory
test_support.TReducer = TReducer
test_support.TMapper = TMapper

test_support.TReducerWithCounters = TReducerWithCounters

def suite():
    test_support.suite()

if __name__ == '__main__':
    _RUNNER = unittest.TextTestRunner(verbosity=2)
    _RUNNER.run((suite()))
