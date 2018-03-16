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

"""\
Generate a GraySort input data set.
The user specifies the number of rows and the output directory and this
class runs a map/reduce program to generate the data.
The format of the data is:

 * (10 bytes key) (constant 2 bytes) (32 bytes rowid)
   (constant 4 bytes) (48 bytes filler) (constant 4 bytes)
 * The rowid is the right justified row id as a hex number.
"""

import struct

import random
import pydoop.mapreduce.api as api
import pydoop.mapreduce.pipes as pp
from ioformats import Writer

TERAGEN = "TERAGEN"
CHECKSUM = "CHECKSUM"

SEED = 423849
CACHE_SIZE = 16 * 1024

getrandbits = random.getrandbits


class GenSort(object):
    """\
    Some sort of gensort look-alike. No idea on its statistical properties
    """
    BREAK_BYTES = struct.pack("2B", 0x00, 0x11)
    DATA_HEAD = struct.pack("4B", 0x88, 0x99, 0xAA, 0xBB)
    DATA_TAIL = struct.pack("4B", 0xCC, 0xDD, 0xEE, 0xFF)

    def __init__(self, seed, row, cache_size):
        self.cache_size = cache_size
        self.fmt = '0%dx' % (2 * self.cache_size)
        self.row = row
        self.cache = None
        self.index = 0
        # we use 10 (keys) + 6 (filler) random bytes per record
        self.skip_ahead(16 * row)
        random.seed(seed)

    def update_cache(self):
        r = getrandbits(8 * self.cache_size)
        self.cache = format(r, self.fmt).encode('ascii')

    def skip_ahead(self, skip):
        """\
        Skip ahead skip random bytes
        """
        chunks = skip // self.cache_size
        cache_size_bits = 8 * self.cache_size
        for _ in range(chunks):
            getrandbits(cache_size_bits)
        self.update_cache()
        self.index = 2 * (skip - chunks * self.cache_size)

    def next_random_block(self):
        if self.index == 2 * self.cache_size:
            self.update_cache()
            self.index = 0
        s, self.index = self.index, self.index + 32
        return self.cache[s:self.index]

    def generate_record(self):
        # 10 bytes of random
        # 2 constant bytes
        # 32 bytes record number as an ASCII-encoded 32-digit hexadecimal
        # 4 bytes of break data
        # 48 bytes of filler based on low 48 bits of random
        # 4 bytes of break data
        rnd = self.next_random_block()
        key = rnd[:10]
        low = rnd[-12:]
        row_id = format(self.row, '032x').encode('ascii')
        filler = bytes(sum(map(list, zip(low, low, low, low)), []))
        value = (self.BREAK_BYTES + row_id +
                 self.DATA_HEAD + filler + self.DATA_TAIL)
        self.row = self.row + 1
        return key, value


class Mapper(api.Mapper):

    def __init__(self, context):
        super(Mapper, self).__init__(context)
        self.gensort = None

    def map(self, context):
        if self.gensort is None:
            row = struct.unpack('>q', context.key)[0]
            self.gensort = GenSort(SEED, row, CACHE_SIZE)
        key, value = self.gensort.generate_record()
        context.emit(key, value)


factory = pp.Factory(mapper_class=Mapper, record_writer_class=Writer)


def __main__():
    pp.run_task(factory, auto_serialize=False)
