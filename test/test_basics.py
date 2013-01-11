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
