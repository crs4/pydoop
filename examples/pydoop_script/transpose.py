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

"""
Transpose a tab-separated text matrix.

  pydoop script transpose.py matrix.txt t_matrix
  hadoop fs -get t_matrix{,}
  sort -mn -k1,1 -o t_matrix.txt t_matrix/part-0000*

t_matrix.txt contains an additional first column with row indexes --
this might not be a problem if it acts as input for another job.
"""

import struct

def mapper(key, value, writer):
  value = value.split()
  for i, a in enumerate(value):
    writer.emit(struct.pack(">q", i), "%s%s" % (key, a))

def reducer(key, ivalue, writer):
  vector = [(struct.unpack(">q", v[:8])[0], v[8:]) for v in ivalue]
  vector.sort()
  writer.emit(struct.unpack(">q", key)[0], "\t".join(v[1] for v in vector))
