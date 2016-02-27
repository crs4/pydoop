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

import re

import pydoop.mapreduce.api as api
import pydoop.mapreduce.pipes as pp


class Mapper(api.Mapper):

    def map(self, context):
        words = re.sub('[^0-9a-zA-Z]+', ' ', context.getInputValue()).split()
        for w in words:
            context.emit(w, 1)


class Reducer(api.Reducer):

    def reduce(self, context):
        s = sum(context.values)
        context.emit(context.key, s)

factory = pp.Factory(mapper_class=Mapper, reducer_class=Reducer)


def __main__():
    pp.run_task(factory)
