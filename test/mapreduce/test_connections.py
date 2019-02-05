# BEGIN_COPYRIGHT
#
# Copyright 2009-2019 CRS4.
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

import io
import os
import unittest

import pydoop.mapreduce.api as api
import pydoop.mapreduce.binary_protocol as bp
import pydoop.mapreduce.pipes as pipes
import pydoop.sercore as sercore
from pydoop.test_utils import WDTestCase

THIS_DIR = os.path.dirname(os.path.abspath(__file__))
M_NAME, R_NAME = "m_task.cmd", "r_task.cmd"


class Mapper(api.Mapper):

    def map(self, context):
        context.emit(context.key, context.value)


class Reducer(api.Reducer):

    def reduce(self, context):
        context.emit(context.key, sum(context.values))


# move to test_utils?
class UplinkDumpReader(object):

    def __init__(self, stream):
        self.stream = stream

    def close(self):
        self.stream.close()

    def __next__(self):
        cmd = self.stream.read_vint()
        if cmd == bp.AUTHENTICATION_RESP:
            return cmd, self.stream.read_tuple("b")
        elif cmd == bp.OUTPUT:
            return cmd, self.stream.read_tuple("bb")
        elif cmd == bp.PARTITIONED_OUTPUT:
            return cmd, self.stream.write_tuple("ibb")
        elif cmd == bp.STATUS:
            return cmd, self.stream.read_tuple("s")
        elif cmd == bp.PROGRESS:
            return cmd, self.stream.read_tuple("f")
        elif cmd == bp.REGISTER_COUNTER:
            return cmd, self.stream.read_tuple("iss")
        elif cmd == bp.INCREMENT_COUNTER:
            return cmd, self.stream.read_tuple("il")
        elif cmd == bp.DONE:
            raise StopIteration
        else:
            raise RuntimeError("unknown command: %d" % cmd)

    def __iter__(self):
        return self

    # py2 compat
    def next(self):
        return self.__next__()


class TestFileConnection(WDTestCase):

    def test_map(self):
        factory = pipes.Factory(Mapper)
        self.__run_test(M_NAME, factory, private_encoding=False)

    def test_reduce(self):
        factory = pipes.Factory(Mapper, reducer_class=Reducer)
        self.__run_test(R_NAME, factory)

    def __run_test(self, name, factory, **kwargs):
        orig_path = os.path.join(THIS_DIR, name)
        cmd_path = os.path.join(self.wd, name)
        with io.open(orig_path, "rb") as fi, io.open(cmd_path, "wb") as fo:
            fo.write(fi.read())
        os.environ["mapreduce.pipes.commandfile"] = cmd_path
        pipes.run_task(factory, **kwargs)
        out_cmd_path = "%s.out" % cmd_path
        self.assertTrue(os.path.exists(out_cmd_path))
        with sercore.FileInStream(out_cmd_path) as stream:
            out_cmds = set(cmd for cmd, _ in UplinkDumpReader(stream))
        self.assertEqual(out_cmds, {bp.OUTPUT, bp.PROGRESS})


def suite():
    suite_ = unittest.TestSuite()
    suite_.addTest(TestFileConnection('test_map'))
    suite_.addTest(TestFileConnection('test_reduce'))
    return suite_


if __name__ == '__main__':
    _RUNNER = unittest.TextTestRunner(verbosity=2)
    _RUNNER.run((suite()))
