#!/usr/bin/env python

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

import sys
import abc
from collections import Counter

import pydoop.mapreduce.api as api
import pydoop.mapreduce.pipes as pp
from pydoop.avrolib import AvroContext


class ColorPickBase(api.Mapper):

    @abc.abstractmethod
    def get_user(self, ctx):
        """
        Get the user record.  This is just to avoid writing near identical
        examples for the various key/value cases.  In a real application,
        carrying records over keys or values would be a design decision,
        so you would simply do, e.g., ``user = self.value``.
        """

    def map(self, ctx):
        user = self.get_user(ctx)
        color = user['favorite_color']
        if color is not None:
            ctx.emit(user['office'], Counter({color: 1}))


class AvroKeyColorPick(ColorPickBase):

    def get_user(self, ctx):
        return ctx.key


class AvroValueColorPick(ColorPickBase):

    def get_user(self, ctx):
        return ctx.value


class AvroKeyValueColorPick(ColorPickBase):

    def get_user(self, ctx):
        return ctx.key

    def map(self, ctx):
        sys.stdout.write("value (unused): %r\n" % (ctx.value,))
        super(AvroKeyValueColorPick, self).map(ctx)


class ColorCountBase(api.Reducer):

    def reduce(self, ctx):
        s = sum(ctx.values, Counter())
        self.emit(s, ctx)

    @abc.abstractmethod
    def emit(self, s, ctx):
        """
        Emit the sum to the ctx.  As in the base mapper, this is just to
        avoid writing near identical examples.
        """


class NoAvroColorCount(ColorCountBase):

    def emit(self, s, ctx):
        ctx.emit(ctx.key, "%r" % s)


class AvroKeyColorCount(ColorCountBase):

    def emit(self, s, ctx):
        ctx.emit({'office': ctx.key, 'counts': s}, ctx.key)


class AvroValueColorCount(ColorCountBase):

    def emit(self, s, ctx):
        ctx.emit(ctx.key, {'office': ctx.key, 'counts': s})


class AvroKeyValueColorCount(ColorCountBase):

    def emit(self, s, ctx):
        record = {'office': ctx.key, 'counts': s}
        ctx.emit(record, record)  # FIXME: do something fancier


def run_task(mapper_class, reducer_class=NoAvroColorCount):
    pp.run_task(
        pp.Factory(mapper_class=mapper_class, reducer_class=reducer_class),
        private_encoding=True, context_class=AvroContext
    )
