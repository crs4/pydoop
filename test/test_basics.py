import unittest
import random

import sys

#----------------------------------------------------------------------------
import hadoop_pipes
#----------------------------------------------------------------------------


class basics_tc(unittest.TestCase):
  def const_ref(self):
    # scope of a string ref
    h = "hello"
    a = hadoop_pipes.double_a_string(h)
    print a

#----------------------------------------------------------------------------
def suite():
  suite = unittest.TestSuite()
  #--
  suite.addTest(basics_tc('const_ref'))
  #--
  return suite

if __name__ == '__main__':
  runner = unittest.TextTestRunner(verbosity=2)
  runner.run((suite()))

