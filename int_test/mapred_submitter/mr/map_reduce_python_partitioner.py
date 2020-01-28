#!/usr/bin/env python

# BEGIN_COPYRIGHT
#
# Copyright 2009-2020 CRS4.
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

from hashlib import md5

import pydoop.mapreduce.api as api
import pydoop.mapreduce.pipes as pipes


class Mapper(api.Mapper):

    def map(self, context):
        for w in context.value.split():
            context.emit(w, 1)


class Reducer(api.Reducer):

    def reduce(self, context):
        context.emit(context.key, sum(context.values))


class Partitioner(api.Partitioner):

    def partition(self, key, n_reduces):
        return int(md5(key).hexdigest(), 16) % n_reduces


def __main__():
    pipes.run_task(pipes.Factory(
        Mapper,
        reducer_class=Reducer,
        partitioner_class=Partitioner
    ))


if __name__ == "__main__":
    __main__()
