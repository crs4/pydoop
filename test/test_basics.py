# BEGIN_COPYRIGHT
# END_COPYRIGHT

import unittest

import pydoop
pp = pydoop.import_version_specific_module('_pipes')


class basics_tc(unittest.TestCase):
  
  def const_ref(self):
    # scope of a string ref
    h = "hello"
    a = pp.double_a_string(h)
    print a

  def create_and_destroy(self):
    class t_m(pp.Mapper):
      def __init__(self, c):
        pp.Mapper.__init__(self)
        self.c = c
    x = [t_m(i) for i in range(10)]


def suite():
  suite = unittest.TestSuite()
  suite.addTest(basics_tc('const_ref'))
  suite.addTest(basics_tc('create_and_destroy'))
  return suite


if __name__ == '__main__':
  runner = unittest.TextTestRunner(verbosity=2)
  runner.run((suite()))
