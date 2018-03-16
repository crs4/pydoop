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

"""\
Transpose a tab-separated text matrix.

  pydoop script transpose.py matrix.txt t_matrix
  hadoop fs -get t_matrix{,}
  sort -mn -k1,1 -o t_matrix.txt t_matrix/part-0000*

t_matrix.txt contains an additional first column with row indexes --
this might not be a problem if it acts as input for another job.

How does it work? Suppose you want to transpose the following matrix:

  a00 a01 a02
  a10 a11 a12

We can set the intermediate key to the column index to have the
framework automatically regroup elements by column. We also have to
send the row index: the reducer will need it to sort each output
row. Although we don't know the global row index for a given input
record, we can use the input key, which is equal to the global byte
count (with the default TextInputFormat). The key/value stream emitted
by the mappers looks like:

  0, (0, 'a00')
  1, (0, 'a01')
  2, (0, 'a02')
  0, (12, 'a10')
  1, (12, 'a11')
  2, (12, 'a12')

And reducers will get:

  0, [(0, 'a00'), (12, 'a10')]
  2, [(0, 'a02'), (12, 'a12')]
  1, [(12, 'a11'), (0, 'a01')]

Writing out the key (i.e., the output row index) together with the
value allows to put the output rows in the correct order.
"""


def mapper(key, value, writer):
    # work around pipes' current limitation with explicit input formats
    try:
        value = value.decode("ascii")
    except AttributeError:
        pass
    for i, a in enumerate(value.split()):
        writer.emit(i, (key, a))


def reducer(key, ivalue, writer):
    row = [_[1] for _ in sorted(ivalue)]
    writer.emit(key, "\t".join(row))
