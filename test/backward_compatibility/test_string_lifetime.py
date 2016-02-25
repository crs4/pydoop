# BEGIN_COPYRIGHT
#
# Copyright 2009-2016 CRS4.
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

import gc


class str_lifetime_tc(unittest.TestCase):

    def memory_stress(self):
        N = 10000
        S = 10000000
        for x in xrange(N):
            pp.create_a_string(S)

    def push_leak_map_context(self):
        N = 100
        S = 100000000
        big_string = 'a' * S
        d = {'input_key': 'foo_key',
             'input_value': big_string,
             'input_split': '',
             'input_key_class': 'foo_key_class',
             'input_value_class': 'foo_value_class',
             'job_conf': {}}
        for x in xrange(N):
            mctx = pp.get_MapContext_object(d)
            mctx.getInputValue()

    def garbage_collect(self):
        N = 10000
        S = 10000000
        for x in xrange(N):
            pp.create_a_string(S)
            gc.collect()


def suite():
    suite = unittest.TestSuite()
    suite.addTest(str_lifetime_tc('push_leak_map_context'))
    # suite.addTest(str_lifetime_tc('garbage_collect'))
    return suite


if __name__ == '__main__':
    runner = unittest.TextTestRunner(verbosity=2)
    runner.run((suite()))
