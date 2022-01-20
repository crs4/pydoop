#!/usr/bin/env python

# BEGIN_COPYRIGHT
#
# Copyright 2009-2022 CRS4.
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
import pydoop.mapreduce.pipes as pipes


class Mapper(api.Mapper):

    # in this case there's no need to serialize/deserialize
    # key is not used, and bytes objects can be split just like strings
    def map(self, context):
        # key = struct.unpack(">q", context.key)[0]
        # value = context.value.decode("utf-8")
        for word in context.value.split():
            context.emit(word, b"1")


class Reducer(api.Reducer):

    def reduce(self, context):
        s = sum(int(_) for _ in context.values)
        context.emit(context.key, b"%d" % s)


def __main__():
    factory = pipes.Factory(Mapper, reducer_class=Reducer)
    pipes.run_task(
        factory,
        raw_keys=True,
        raw_values=True,
        private_encoding=False,
        auto_serialize=False,
    )


if __name__ == "__main__":
    __main__()
