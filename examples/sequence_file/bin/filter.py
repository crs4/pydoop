#!/usr/bin/env python

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

"""
Filter out words whose occurrence falls below a specified value.
"""

import struct

from pydoop.mapreduce.pipes import run_task, Factory
from pydoop.mapreduce.api import Mapper, Reducer


class FilterMapper(Mapper):
    """
    Process a wordcount output stream, emitting only records relative to
    words whose count is equal to or above the configured threshold.
    """
    def __init__(self, context):
        super(FilterMapper, self).__init__(context)
        jc = context.job_conf
        self.threshold = jc.get_int("filter.occurrence.threshold")

    def map(self, context):
        word, occurrence = context.key, context.value
        occurrence = struct.unpack(">i", occurrence)[0]
        if occurrence >= self.threshold:
            context.emit(word, str(occurrence))


class FilterReducer(Reducer):

    def reduce(self, context):
        pass


if __name__ == "__main__":
    run_task(Factory(FilterMapper, FilterReducer))
