# BEGIN_COPYRIGHT
# 
# Copyright 2009-2013 CRS4.
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

from struct import pack


def serialize_int(i, stream):
  serialize_long(i, stream)


def serialize_long(i, stream):
  if i >= -112 and i <= 127:
    stream.write(pack("B", i))
    return
  length = -112
  if i < 0:
    i ^= -111
    length = -120
  tmp = i
  while tmp != 0:
    tmp >>= 8
    length -= 1
  stream.write(pack("B", length))
  length = - (length + 120) if length < -120 else - (length + 112)
