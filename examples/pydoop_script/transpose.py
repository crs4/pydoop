# BEGIN_COPYRIGHT
# 
# Copyright 2012 CRS4.
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
Matrix transposer
"""

import struct

def mapper(key, value, writer):
  value = value.split()
  for i, a in enumerate(value):
    writer.emit(struct.pack(">q", i), "%s\t%s" % (key, a))

def reducer(key, ivalue, writer):
  vector = []
  for v in ivalue:
    v = v.split("\t")
    v[0] = struct.unpack(">q", v[0])[0]
    vector.append(v)
  vector.sort()
  vector = [v[1] for v in vector]
  writer.emit(struct.unpack(">q", key)[0], "\t".join(vector))
