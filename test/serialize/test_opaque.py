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

import unittest
import os
import shutil
import uuid

import pydoop
from pydoop.hdfs import hdfs
from pydoop.utils.serialize import (
    OpaqueInputSplit, write_opaques, read_opaques)

import pydoop.test_utils as utils

_JAVA_SRC_ROOT = 'it'
_OPAQUE_ROUNDTRIP_CLASS = 'it.crs4.pydoop.mapreduce.pipes.opaque_roundtrip'
_OPAQUE_ROUNDTRIP_SRC = 'it/crs4/pydoop/mapreduce/pipes/opaque_roundtrip.java'


class TestOpaqueInputSplit(unittest.TestCase):

    def setUp(self):
        self.fs = hdfs()
        self.wd = utils.make_wd(self.fs)

    def tearDown(self):
        self.fs.delete(self.wd)
        self.fs.close()

    def _make_random_path(self, where=None):
        return "%s/%s_%s" % (where or self.wd, uuid.uuid4().hex, utils.UNI_CHR)

    def _generate_opaque_splits(self, n):
        return [OpaqueInputSplit('{}_code'.format(_), '{}_payload'.format(_))
                for _ in range(n)]

    def _test_opaque(self, o, no):
        self.assertEqual(o.code, no.code)
        self.assertEqual(o.payload, no.payload)

    def _test_opaques(self, opaques, nopaques):
        self.assertEqual(len(opaques), len(nopaques))
        for o, no in zip(opaques, nopaques):
            self._test_opaque(o, no)

    def _run_java(self, in_uri, out_uri, wd):
        this_directory = os.path.abspath(os.path.dirname(__file__))
        shutil.copytree(os.path.join(this_directory, _JAVA_SRC_ROOT),
                        os.path.join(wd, _JAVA_SRC_ROOT))
        classpath = '.:%s:%s:%s' % (
            wd, pydoop.jar_path(), pydoop.hadoop_classpath())
        src = os.path.join(wd, _OPAQUE_ROUNDTRIP_SRC)
        utils.compile_java(src, classpath)
        utils.run_java(
            _OPAQUE_ROUNDTRIP_CLASS, classpath, [in_uri, out_uri], wd)

    def _do_java_roundtrip(self, splits, wd='/tmp'):
        in_uri = self._make_random_path()
        out_uri = self._make_random_path()
        with self.fs.open_file(in_uri, 'wb') as f:
            write_opaques(splits, f)
        self._run_java(in_uri, out_uri, wd)
        with self.fs.open_file(out_uri, 'rb') as f:
            nsplits = read_opaques(f)
        return nsplits

    def test_opaque(self):
        code = "acode222"
        payload = {'a': 33, 'b': "333"}
        o = OpaqueInputSplit(code, payload)
        self.assertEqual(code, o.code)
        self.assertEqual(payload, o.payload)
        fname = self._make_random_path('/tmp')
        with open(fname, 'wb') as f:
            o.write(f)
        with open(fname, 'rb') as f:
            no = OpaqueInputSplit()
            no.read(f)
        self._test_opaque(o, no)
        os.unlink(fname)

    def test_write_read_opaques(self):
        n = 10
        opaques = self._generate_opaque_splits(n)
        fname = self._make_random_path('/tmp')
        with open(fname, 'wb') as f:
            write_opaques(opaques, f)
        with open(fname, 'rb') as f:
            nopaques = read_opaques(f)
        self._test_opaques(opaques, nopaques)
        os.unlink(fname)

    def test_opaque_java_round_trip(self):
        n = 10
        splits = self._generate_opaque_splits(n)
        dname = self._make_random_path('/tmp')
        os.mkdir(dname)
        nsplits = self._do_java_roundtrip(splits, wd=dname)
        shutil.rmtree(dname)
        self._test_opaques(splits, nsplits)


def suite():
    return unittest.TestLoader().loadTestsFromTestCase(TestOpaqueInputSplit)


if __name__ == '__main__':
    _RUNNER = unittest.TextTestRunner(verbosity=2)
    _RUNNER.run((suite()))
