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

import unittest
import uuid

from pydoop.hdfs.core import init
hdfs = init()


class TestCore(unittest.TestCase):

    def test_default(self):
        path = "/tmp/pydoop-test-{}".format(uuid.uuid4().hex)
        fs = f = None
        try:
            fs = hdfs.CoreHdfsFs("default", 0)
            f = fs.open_file(path, "w")
            f.write(b"bar\n")
        finally:
            if f:
                f.close()
                fs.delete(path)
            if fs:
                fs.close()


def suite():
    suite_ = unittest.TestSuite()
    suite_.addTest(TestCore('test_default'))
    return suite_


if __name__ == '__main__':
    _RUNNER = unittest.TextTestRunner(verbosity=2)
    _RUNNER.run((suite()))
