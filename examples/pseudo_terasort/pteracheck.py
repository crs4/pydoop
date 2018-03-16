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

import pydoop.mapreduce.api as api
import pydoop.mapreduce.pipes as pp
from ioformats import CheckReader as Reader

import logging

logging.basicConfig()
LOGGER = logging.getLogger("pteracheck")
LOGGER.setLevel(logging.CRITICAL)


class StupidMapper(api.Mapper):

    def __init__(self, context):
        super(StupidMapper, self).__init__(context)
        self.logger = LOGGER.getChild("Mapper")

    def map(self, context):
        self.logger.debug('key: %s, val: %s', context.key, context.value)
        isplit = context.key
        interval = context.value
        context.emit(isplit.filename, (isplit, interval))


class StupidReducer(api.Reducer):

    def __init__(self, context):
        super(StupidReducer, self).__init__(context)
        self.logger = LOGGER.getChild("Reducer")

    def reduce(self, context):
        fname = context.key
        recs = sorted(context.values, key=lambda _: _[0].offset)
        offset, length = recs[0][0].offset, recs[0][0].length
        lbndry, rbndry = recs[0][1]
        for r in recs[1:]:
            assert r[0].offset == offset + length
            assert rbndry <= r[1][0]
            offset, length = r[0].offset, r[0].length
            rbndry = r[1][1]
        context.emit(fname, [lbndry, rbndry])


factory = pp.Factory(
    mapper_class=StupidMapper,
    reducer_class=StupidReducer,
    record_reader_class=Reader,
)


def __main__():
    pp.run_task(factory)
