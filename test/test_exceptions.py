# BEGIN_COPYRIGHT
# END_COPYRIGHT

import unittest
import random

import sys

#----------------------------------------------------------------------------
import pydoop
pp = pydoop.import_version_specific_module('_pipes')
#----------------------------------------------------------------------------

class exceptions_tc(unittest.TestCase):
  def raise_pydoop(self):
    m = "hello there!"
    self.assertRaises(UserWarning, pp.raise_pydoop_exception, m)
    try:
      pp.raise_pydoop_exception(m)
    except Exception, e:
      self.assertEqual(e.args[0], 'pydoop_exception: ' + m)

  def raise_pipes(self):
    m = "hello there!"
    self.assertRaises(UserWarning, pp.raise_pipes_exception, m)
    try:
      pp.raise_pipes_exception(m)
    except Exception, e:
      self.assertEqual(e.args[0], 'pydoop_exception.pipes: ' + m)

#----------------------------------------------------------------------------
def suite():
  suite = unittest.TestSuite()
  #--
  suite.addTest(exceptions_tc("raise_pydoop"))
  suite.addTest(exceptions_tc("raise_pipes"))
  #--
  return suite

if __name__ == '__main__':
  runner = unittest.TextTestRunner(verbosity=2)
  runner.run((suite()))

