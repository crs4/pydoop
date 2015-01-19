#!/usr/bin/env python

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

"""
Minimal word count example.
"""

import re

from pydoop.mapreduce.pipes import run_task, Factory
from pydoop.mapreduce.api import Mapper, Reducer

import logging

logging.basicConfig()
LOGGER = logging.getLogger('wc-minimal')
LOGGER.setLevel(logging.CRITICAL)


class TMapper(Mapper):

    def __init__(self, ctx):
        super(TMapper, self).__init__(ctx)
        self.ctx = ctx
        LOGGER.info("Mapper instantiated")

    def map(self, ctx):
        words = re.sub('[^0-9a-zA-Z]+', ' ', ctx.value).split()
        for w in words:
            ctx.emit(w, 1)


class TReducer(Reducer):

    def __init__(self, ctx):
        super(TReducer, self).__init__(ctx)
        self.ctx = ctx
        LOGGER.info("Reducer instantiated")

    def reduce(self, ctx):
        s = sum(ctx.values)
        # Note: we explicitly write the value as a str.
        ctx.emit(ctx.key, str(s))


FACTORY = Factory(mapper_class=TMapper, reducer_class=TReducer)


def main():
    run_task(FACTORY)

if __name__ == "__main__":
    main()
