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

import os
import unittest
import itertools as it

import avro.datafile as avdf
from avro.io import DatumReader, DatumWriter

from pydoop.mapreduce.pipes import InputSplit
from pydoop.avrolib import (
    SeekableDataFileReader, AvroReader, AvroWriter, parse
)
from pydoop.test_utils import WDTestCase
from pydoop.utils.py3compat import czip, cmap
import pydoop.hdfs as hdfs

from common import avro_user_record


THIS_DIR = os.path.dirname(os.path.abspath(__file__))


class TestAvroIO(WDTestCase):

    def setUp(self):
        super(TestAvroIO, self).setUp()
        with open(os.path.join(THIS_DIR, "user.avsc")) as f:
            self.schema = parse(f.read())

    def write_avro_file(self, rec_creator, n_samples, sync_interval):
        avdf.SYNC_INTERVAL = sync_interval
        self.assertEqual(avdf.SYNC_INTERVAL, sync_interval)
        fo = self._mkf('data.avro', mode='wb')
        with avdf.DataFileWriter(fo, DatumWriter(), self.schema) as writer:
            for i in range(n_samples):
                writer.append(rec_creator(i))
        return fo.name

    def test_seekable(self):
        fn = self.write_avro_file(avro_user_record, 500, 1024)
        with open(fn, 'rb') as f:
            sreader = SeekableDataFileReader(f, DatumReader())
            res = [t for t in czip(cmap(
                lambda _: f.tell(), it.repeat(1)
            ), sreader)]
            sreader.align_after(res[-1][0])
            with self.assertRaises(StopIteration):
                r = next(sreader)
            sreader.align_after(0)
            r = next(sreader)
            self.assertEqual(r, res[0][1])

            def offset_iterator():
                s = -1
                for o, r in res:
                    sreader.align_after(o)
                    t = f.tell()
                    if t == s:
                        continue
                    s = t
                    x = next(sreader)
                    yield (t, x)

            i = 0
            for xo, x in offset_iterator():
                sreader.align_after(xo)
                for o, r in res[i:]:
                    if o >= xo:
                        self.assertEqual(x, r)
                        break
                    i += 1

    def test_avro_reader(self):

        N = 500
        fn = self.write_avro_file(avro_user_record, N, 1024)
        url = hdfs.path.abspath(fn, local=True)

        class FunkyCtx(object):
            def __init__(self, isplit):
                self.input_split = isplit

        def get_areader(offset, length):
            isplit = InputSplit(InputSplit.to_string(url, offset, length))
            ctx = FunkyCtx(isplit)
            return AvroReader(ctx)

        areader = get_areader(0, 14)
        file_length = areader.reader.file_length
        with self.assertRaises(StopIteration):
            next(areader)
        areader = get_areader(0, file_length)
        with SeekableDataFileReader(open(fn, 'rb'), DatumReader()) as sreader:
            for (o, a), s in czip(areader, sreader):
                self.assertEqual(a, s)
        mid_len = int(file_length / 2)
        lows = [x for x in get_areader(0, mid_len)]
        highs = [x for x in get_areader(mid_len, file_length)]
        self.assertEqual(N, len(lows) + len(highs))

    def test_avro_writer(self):

        class FunkyCtx(object):

            def __init__(self_, job_conf):
                self_.job_conf = job_conf

        class AWriter(AvroWriter):

            schema = self.schema

            def emit(self_, key, value):
                self_.writer.append(key)

        ctx = FunkyCtx({
            'mapreduce.task.partition': 1,
            'mapreduce.task.output.dir': hdfs.path.abspath(self.wd, local=True)
        })
        awriter = AWriter(ctx)
        N = 10
        for i in range(N):
            awriter.emit(avro_user_record(i), '')
        awriter.close()


def suite():
    suite_ = unittest.TestSuite()
    suite_.addTest(TestAvroIO('test_seekable'))
    suite_.addTest(TestAvroIO('test_avro_reader'))
    suite_.addTest(TestAvroIO('test_avro_writer'))
    return suite_


if __name__ == '__main__':
    _RUNNER = unittest.TextTestRunner(verbosity=2)
    _RUNNER.run((suite()))
