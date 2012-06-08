# BEGIN_COPYRIGHT
# 
# Copyright 2012 CRS4.
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
import pydoop.hadoop_utils as hu
from pydoop.hadoop_utils import HadoopVersionError


class pydoop_tc(unittest.TestCase):

  def test_hadoop_version(self):
    ver = pydoop.hadoop_version_info()
    self.assertTrue(all(type(_) == int for _ in ver[:3]))

  def test_version_tuple_with_good_strings(self):
    good_test_cases = [
      "0.20.3-cdh3",
      "0.20.2",
      "0.21.2",
      '0.20.203.1-SNAPSHOT'
    ]
    for s in good_test_cases:
      self.assertTrue(hu.version_tuple(s) is not None)

  def test_version_tuple_with_bad_strings(self):
    bad_test_cases = [
      "0",
      "0.20",
      "bla",
      '0.20.str'
    ]
    for s in bad_test_cases:
      self.assertRaises(HadoopVersionError, hu.version_tuple, s)

  def test_version(self):
    ver = pydoop.hadoop_version_info()
    self.assertTrue(ver is not None)
    self.assertTrue(len(ver) >= 3)
    self.assertTrue(all(type(_) == int for _ in ver[:3]))

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
      self.assertEqual(pydoop.__jar_name__, filename)
      # ensure the parent directory is called pydoop
      self.assertEqual('pydoop', os.path.basename(directory))


def suite():
  suite = unittest.TestSuite()
  suite.addTest(pydoop_tc('test_hadoop_version'))
  suite.addTest(pydoop_tc('test_version_tuple_with_good_strings'))
  suite.addTest(pydoop_tc('test_version_tuple_with_bad_strings'))
  suite.addTest(pydoop_tc('test_version'))
  suite.addTest(pydoop_tc('test_home'))
  suite.addTest(pydoop_tc('test_conf'))
  suite.addTest(pydoop_tc('test_pydoop_jar_path'))
  return suite


if __name__ == '__main__':
  runner = unittest.TextTestRunner(verbosity=2)
  runner.run((suite()))
