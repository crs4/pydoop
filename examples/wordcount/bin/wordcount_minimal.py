#!/usr/bin/env python

# BEGIN_COPYRIGHT
#
# Copyright 2009-2017 CRS4.
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

"""\
Includes only the bare minimum required to run wordcount. See
wordcount-full.py for an example that uses counters, RecordReader, etc.
"""

import re

import pydoop.mapreduce.pipes as pipes
import pydoop.mapreduce.api as api


class Mapper(api.Mapper):

    def map(self, context):
        words = re.sub(b'[^0-9a-zA-Z]+', b' ', context.value).split()
        for w in words:
            context.emit(w, 1)


class Reducer(api.Reducer):

    def reduce(self, context):
        s = sum(context.values)
        context.emit(context.key, str(s))


FACTORY = pipes.Factory(mapper_class=Mapper, reducer_class=Reducer)


def main():
    pipes.run_task(FACTORY)


if __name__ == "__main__":
    main()
