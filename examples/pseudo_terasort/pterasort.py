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

import os
import io
import itertools as it
from concurrent.futures import ThreadPoolExecutor
from bintrees.avltree import AVLTree

import pydoop.mapreduce.api as api
import pydoop.mapreduce.pipes as pp
import pydoop.hdfs as hdfs
import pydoop.utils.serialize as srl
from ioformats import Reader, Writer, KEY_LENGTH, RECORD_LENGTH

import logging

logging.basicConfig()
LOGGER = logging.getLogger("pterasort")
LOGGER.setLevel(logging.CRITICAL)


class Selector(AVLTree):
    def select_partition(self, key):
        node = self._root
        while node is not None:
            if key == node.key:
                return node.value
            elif key < node.key:
                candidate = node.value - 1
                node = node.left
            else:
                candidate = node.value
                node = node.right
        return candidate


class Partitioner(api.Partitioner):

    BREAK_POINTS_CACHE_FILE = '__break_point_cache_file'
    TMP_DIR = '/tmp'

    @classmethod
    def _choose_break_points(cls, args):
        n_records, n_breakpoints, path = args
        block_size = n_records * RECORD_LENGTH
        with hdfs.open(path, 'r') as f:
            data = f.read(block_size)
        assert len(data) == block_size
        step = max(n_records // n_breakpoints, 1)
        keys = sorted([data[k:k + KEY_LENGTH]
                       for k in range(0, block_size, RECORD_LENGTH)])
        return [_ for _ in it.islice(keys, step, n_records, step)]

    @classmethod
    def get_break_points(cls, n_records, n_reducers, paths, n_threads=2):
        def find_center(keys):
            return sorted(keys)[len(keys) // 2]
        with ThreadPoolExecutor(n_threads) as p:
            return map(find_center,
                       zip(*[bk for bk in p.map(cls._choose_break_points,
                                                [(n_records, n_reducers, _)
                                                 for _ in paths])]))

    @classmethod
    def initialize_break_points(cls, n_reducers, sampled_records,
                                input_dir, n_threads=2):
        file_infos = [i for i in hdfs.lsl(input_dir)
                      if (i['kind'] == 'file' and
                          os.path.basename(i['name']).startswith('part'))]
        n_files = len(file_infos)
        total_size = sum(map(lambda _: int(_['size']), file_infos))
        n_records = total_size // RECORD_LENGTH
        assert n_records > sampled_records
        df = max(n_files // n_reducers, 1)
        paths = [i['name']
                 for i in it.islice(file_infos, 0, df * n_reducers, df)]
        break_points = cls.get_break_points(sampled_records // n_reducers,
                                            n_reducers, paths, n_threads)
        vals = [_ for _ in zip(break_points, range(1, n_reducers))]
        selector = Selector(vals)
        bp_path = os.path.join(cls.TMP_DIR, cls.BREAK_POINTS_CACHE_FILE)
        with io.open(bp_path, "wb") as f:
            f.write(srl.private_encode(selector))
        return bp_path

    def __init__(self, context):
        super(Partitioner, self).__init__(context)
        self.logger = LOGGER.getChild("Partitioner")
        with io.open(self.BREAK_POINTS_CACHE_FILE, 'rb') as f:
            self.selector = srl.private_decode(f.read())

    def partition(self, key, n_reduces):
        part = self.selector.select_partition(key)
        self.logger.debug('key: %s, part: %s', key, part)
        return part


class StupidMapper(api.Mapper):
    def __init__(self, context):
        super(StupidMapper, self).__init__(context)
        self.logger = LOGGER.getChild("Mapper")

    def map(self, context):
        self.logger.debug('key: %s, val: %s', context.key, context.value)
        context.emit(context.key, context.value)


class StupidReducer(api.Reducer):
    def reduce(self, context):
        key = context.key
        for v in context.values:
            context.emit(key, v)


factory = pp.Factory(
    mapper_class=StupidMapper,
    reducer_class=StupidReducer,
    partitioner_class=Partitioner,
    record_reader_class=Reader,
    record_writer_class=Writer,
)


def __main__():
    pp.run_task(factory, private_encoding=False, auto_serialize=False)
