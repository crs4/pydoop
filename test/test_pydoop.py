# BEGIN_COPYRIGHT
# END_COPYRIGHT

import unittest
import os

import pydoop
import pydoop.hadoop_utils as hu
from pydoop.hadoop_utils import HadoopVersionError


class pydoop_tc(unittest.TestCase):
  def test_get_hadoop_version(self):
    tp = hu.get_hadoop_version(pydoop.hadoop_home())
    self.assertTrue( all([ type(t) == int for t in tp[0:3] ]) )

  def test_version_tuple_with_good_strings(self):
    good_test_cases = [
      "0.20.3-cdh3",
      "0.20.2",
      "0.21.2",
      '0.20.203.1-SNAPSHOT'
    ]
    for s in good_test_cases:
      self.assertTrue( hu.version_tuple(s) is not None )

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
    ver = pydoop.hadoop_version()
    self.assertTrue(ver is not None)
    self.assertTrue(len(ver) >= 3)
    self.assertTrue( all([ type(v) == int for v in ver[0:3]]) )

  def test_home(self):
    if os.environ.has_key('HADOOP_HOME'):
      self.assertEqual(os.environ['HADOOP_HOME'], pydoop.hadoop_home())

  def test_conf(self):
    if os.environ.has_key('HADOOP_CONF_DIR'):
      self.assertEqual(os.environ['HADOOP_CONF_DIR'], pydoop.hadoop_conf())

def suite():
  suite = unittest.TestSuite()
  suite.addTest(pydoop_tc('test_get_hadoop_version'))
  suite.addTest(pydoop_tc('test_version_tuple_with_good_strings'))
  suite.addTest(pydoop_tc('test_version_tuple_with_bad_strings'))
  suite.addTest(pydoop_tc('test_version'))
  suite.addTest(pydoop_tc('test_home'))
  suite.addTest(pydoop_tc('test_conf'))
  return suite


if __name__ == '__main__':
  runner = unittest.TextTestRunner(verbosity=2)
  runner.run((suite()))
