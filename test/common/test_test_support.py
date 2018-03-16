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
Test suite for pydoop.test_support
"""

import unittest
import uuid
import os

import pydoop.test_support as pts

TARGET_CODE = """\
#!/foo/bar/python

from __future__ import print_function
from future import whatever
import foobar

print("Hello, world")
"""


class TestTestSupport(unittest.TestCase):

    def test_inject_code(self):
        lines = TARGET_CODE.splitlines()
        new_code = uuid.uuid4().hex
        ret = pts.inject_code(new_code, TARGET_CODE)
        ret_lines = ret.splitlines()
        self.assertEqual(ret_lines[:3], lines[:3])
        self.assertTrue(new_code in ret_lines[3:-4])
        self.assertEqual(ret_lines[-4:], lines[-4:])

    def test_set_python_cmd(self):
        cmd = "/usr/bin/python3"
        ret = pts.set_python_cmd(TARGET_CODE, cmd)
        self.assertEqual(ret.split(os.linesep, 1)[0], "#!%s" % cmd)


def suite():
    suite_ = unittest.TestSuite()
    suite_.addTest(TestTestSupport('test_inject_code'))
    suite_.addTest(TestTestSupport('test_set_python_cmd'))
    return suite_


if __name__ == '__main__':
    _RUNNER = unittest.TextTestRunner(verbosity=2)
    _RUNNER.run((suite()))
