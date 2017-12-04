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
import pydoop.mapreduce.api as api
import pydoop.mapreduce.pipes as pp
import pydoop.hdfs as hdfs
from ioformats import CheckReader as Reader

import os
import io

import logging

logging.basicConfig()
LOGGER = logging.getLogger("pteracheck")
LOGGER.setLevel(logging.CRITICAL)

RECORD_LENGTH = 91
KEY_LENGTH = 10


class StupidMapper(api.Mapper):
    def __init__(self, context):
        super(StupidMapper, self).__init__(context)
        self.logger = LOGGER.getChild("Mapper")

    def map(self, context):
        self.logger.debug('key: %s, val: %s', context.key, context.value)
        context.emit(context.key, context.value)


class StupidReducer(api.Reducer):
    def reduce(self, context):
        isplit = context.key
        for v in context.values:
            context.emit("{} - {} - {}".format(isplit.filename,
                                               isplit.offset, isplit.length),
                         v)


factory = pp.Factory(
    mapper_class=StupidMapper,
    reducer_class=StupidReducer,
    record_reader_class=Reader,
)


def __main__():
    pp.run_task(factory)
