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

import pydoop.hdfs as hdfs


def kv_pair(s):
    return s.split("=", 1)


def a_file_that_can_be_read(x):
    with open(x, 'r'):
        pass
    return x


def a_hdfs_file(x):
    _, _, _ = hdfs.path.split(x)
    return x


def a_comma_separated_list(x):
    # FIXME unclear how does one check for bad lists...
    return x
