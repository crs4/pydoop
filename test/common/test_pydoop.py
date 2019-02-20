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

"""
Test suite for top-level functions.
"""

import unittest
import os
import tempfile
import shutil
from imp import reload

import pydoop


class TestPydoop(unittest.TestCase):

    def setUp(self):
        self.wd = tempfile.mkdtemp(prefix='pydoop_test_')
        self.old_vars = {
            'HADOOP_HOME': os.getenv('HADOOP_HOME'),
            'HADOOP_CONF_DIR': os.getenv('HADOOP_CONF_DIR'),
        }

    def tearDown(self):
        for k, v in self.old_vars.items():
            if v:
                os.environ[k] = v
            else:
                os.environ.pop(k, None)
        reload(pydoop)
        shutil.rmtree(self.wd)

    def test_home(self):
        old_home = pydoop.hadoop_home()
        if os.path.isdir(old_home):
            new_home = os.path.join(self.wd, 'hadoop')
            os.symlink(old_home, new_home)
            os.environ['HADOOP_HOME'] = new_home
            reload(pydoop)
            self.assertEqual(pydoop.hadoop_home(), new_home)

    def test_conf(self):
        old_conf = pydoop.hadoop_conf()
        new_conf = os.path.join(self.wd, "conf")
        shutil.copytree(old_conf, new_conf)
        os.environ['HADOOP_CONF_DIR'] = new_conf
        reload(pydoop)
        self.assertEqual(pydoop.hadoop_conf(), new_conf)

    def test_pydoop_jar_path(self):
        jar_path = pydoop.jar_path()
        if jar_path is not None:
            self.assertTrue(os.path.exists(jar_path))
            directory, filename = os.path.split(jar_path)
            self.assertEqual(filename, pydoop.jar_name())
            self.assertEqual('pydoop', os.path.basename(directory))


def suite():
    suite_ = unittest.TestSuite()
    suite_.addTest(TestPydoop('test_home'))
    suite_.addTest(TestPydoop('test_conf'))
    suite_.addTest(TestPydoop('test_pydoop_jar_path'))
    return suite_


if __name__ == '__main__':
    _RUNNER = unittest.TextTestRunner(verbosity=2)
    _RUNNER.run((suite()))
