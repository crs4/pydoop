# BEGIN_COPYRIGHT
#
# Copyright 2009-2015 CRS4.
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

import avro.schema
import avro.datafile as avdf
from avro.io import DatumReader, DatumWriter

from pydoop.mapreduce.pipes import InputSplit
from pydoop.avrolib import SeekableDataFileReader, AvroReader, AvroWriter

from common import avro_user_record


THIS_DIR = os.path.dirname(os.path.abspath(__file__))
AVRO_DATA = 'users.avro'


class TestAvroIO(unittest.TestCase):

    def setUp(self):
        with open(os.path.join(THIS_DIR, "user.avsc")) as f:
            self.schema = avro.schema.parse(f.read())

    def write_avro_file(self, file_object, rec_creator, n_samples,
                        sync_interval):
        avdf.SYNC_INTERVAL = sync_interval
        self.assertEqual(avdf.SYNC_INTERVAL, sync_interval)
        writer = avdf.DataFileWriter(file_object, DatumWriter(), self.schema)
        for i in xrange(n_samples):
            writer.append(rec_creator(i))
        writer.close()

    def test_seekable(self):
        with open(AVRO_DATA, 'wb') as f:
            self.write_avro_file(f, avro_user_record, 500, 1024)
        with open(AVRO_DATA, 'rb') as f:
            sreader = SeekableDataFileReader(f, DatumReader())
            res = [t for t in it.izip(it.imap(
                lambda _: f.tell(), it.repeat(1)
            ), sreader)]
            sreader.align_after(res[-1][0])
            with self.assertRaises(StopIteration):
                r = sreader.next()
            sreader.align_after(0)
            r = sreader.next()
            self.assertEqual(r, res[0][1])

            def offset_iterator():
                s = -1
                for o, r in res:
                    sreader.align_after(o)
                    t = f.tell()
                    if t == s:
                        continue
                    s = t
                    x = sreader.next()
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

        class FunkyCtx(object):
            def __init__(self, isplit):
                self.input_split = isplit
        url = '/'.join(['file://', THIS_DIR, AVRO_DATA])

        def get_areader(offset, length):
            isplit = InputSplit(InputSplit.to_string(url, offset, length))
            ctx = FunkyCtx(isplit)
            return AvroReader(ctx)

        N = 500
        with open(AVRO_DATA, 'wb') as f:
            self.write_avro_file(f, avro_user_record, N, 1024)
        areader = get_areader(0, 14)
        file_length = areader.reader.file_length
        with self.assertRaises(StopIteration):
            areader.next()
        areader = get_areader(0, file_length)
        sreader = SeekableDataFileReader(open(AVRO_DATA), DatumReader())
        for (o, a), s in it.izip(areader, sreader):
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

        url = '/'.join(['file://', THIS_DIR])
        ctx = FunkyCtx({
            'mapreduce.task.partition': 1,
            'mapreduce.task.output.dir': url
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
