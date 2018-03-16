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

# DOCS_INCLUDE_START
from collections import Counter

import pydoop.mapreduce.api as api
import pydoop.mapreduce.pipes as pp
from pydoop.avrolib import AvroContext


class Mapper(api.Mapper):

    def map(self, ctx):
        user = ctx.value
        color = user['favorite_color']
        if color is not None:
            ctx.emit(user['office'], Counter({color: 1}))


class Reducer(api.Reducer):

    def reduce(self, ctx):
        s = sum(ctx.values, Counter())
        ctx.emit('', {'office': ctx.key, 'counts': s})


def __main__():
    factory = pp.Factory(mapper_class=Mapper, reducer_class=Reducer)
    pp.run_task(factory, private_encoding=True, context_class=AvroContext)
