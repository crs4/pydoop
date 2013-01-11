# BEGIN_COPYRIGHT
# 
# Copyright 2009-2013 CRS4.
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

import pydoop


class pydoop_tc(unittest.TestCase):

  def test_home(self):
    if os.environ.has_key('HADOOP_HOME'):
      self.assertEqual(os.environ['HADOOP_HOME'], pydoop.hadoop_home())

  def test_conf(self):
    if os.environ.has_key('HADOOP_CONF_DIR'):
      self.assertEqual(os.environ['HADOOP_CONF_DIR'], pydoop.hadoop_conf())

  def test_pydoop_jar_path(self):
    jar_path = pydoop.jar_path()
    if jar_path is not None:
      self.assertTrue(os.path.exists(jar_path))
      directory, filename = os.path.split(jar_path)
      self.assertEqual(filename, pydoop.jar_name())
      self.assertEqual('pydoop', os.path.basename(directory))


def suite():
  suite = unittest.TestSuite()
  suite.addTest(pydoop_tc('test_home'))
  suite.addTest(pydoop_tc('test_conf'))
  suite.addTest(pydoop_tc('test_pydoop_jar_path'))
  return suite


if __name__ == '__main__':
  runner = unittest.TextTestRunner(verbosity=2)
  runner.run((suite()))
