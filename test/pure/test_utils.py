# BEGIN_COPYRIGHT
# 
# Copyright 2009-2014 CRS4.
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

#FIXME
import sys
sys.path.insert(0, '../../')

from pydoop.pure.string_utils import quote_string, unquote_string

class TestUtils(unittest.TestCase):

    def test_quote(self):
        for x in ['dfskjfdjsalk', 'sdkfj\ta\t\n', 'dfssd\t\n', '\adsfsdfa\t\n',
                  'dsjfkjewrwerwerwe8239489238492\n \t dfasd \\',
                  'jdsfkj\\hsdjhfjh\\\t\n']:
            self.assertEqual(x, unquote_string(quote_string(x)))

def suite():
  suite = unittest.TestSuite()
  suite.addTest(TestUtils('test_quote'))
  return suite


if __name__ == '__main__':
  runner = unittest.TextTestRunner(verbosity=2)
  runner.run((suite()))
