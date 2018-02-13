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
import getpass
import tempfile
import os

import pydoop.hdfs as hdfs
from common_hdfs_tests import TestCommon, common_tests


class TestConnection(unittest.TestCase):

    def runTest(self):
        current_user = getpass.getuser()
        cwd = os.getcwd()
        os.chdir(tempfile.gettempdir())
        for user in None, current_user, "nobody":
            expected_user = current_user
            fs = hdfs.hdfs("", 0, user=user)
            self.assertEqual(fs.user, expected_user)
            fs.close()
        os.chdir(cwd)


class TestLocalFS(TestCommon):

    def __init__(self, target):
        TestCommon.__init__(self, target, '', 0)


def suite():
    suite_ = unittest.TestSuite()
    suite_.addTest(TestConnection('runTest'))
    tests = common_tests()
    for t in tests:
        suite_.addTest(TestLocalFS(t))
    return suite_


if __name__ == '__main__':
    _RUNNER = unittest.TextTestRunner(verbosity=2)
    _RUNNER.run((suite()))
