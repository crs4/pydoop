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

import pydoop.mapreduce.api as api
import pydoop.mapreduce.pipes as pp


class Mapper(api.Mapper):

    def map(self, ctx):
        p = ctx.value.strip().split('\t')
        ctx.emit(p[0], p[1])


def __main__():
    pp.run_task(pp.Factory(Mapper, None))


if __name__ == "__main__":
    __main__()
