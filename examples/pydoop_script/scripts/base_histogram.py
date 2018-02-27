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
Count the base frequency in sequencing data (in SAM format).

  input: file in SAM format
  output: tab-separated (base, count) pairs
"""


def mapper(_, samrecord, writer):
    seq = samrecord.split("\t", 10)[9]
    for c in seq:
        writer.emit(c, 1)
    writer.count("bases", len(seq))


def reducer(key, ivalue, writer):
    writer.emit(key, sum(ivalue))
