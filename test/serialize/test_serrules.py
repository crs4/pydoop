# -*- coding: utf-8 -*-
# vim: set fileencoding: utf-8

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
import pydoop.sercore as sc
import pydoop.utils.serialize as us


class RawTrivial(object):
    WR_RULE = ((b'alpha', b's'), (b'beta', b'i'), (b'gamma', b'f'))

    def __init__(self, alpha, beta, gamma):
        self.alpha = alpha
        self.beta = beta
        self.gamma = gamma


class FancyTrivial(object):
    WR_RULE = ((b'alpha', b's'), )

    def __init__(self, alpha, beta, gamma):
        self._alpha = alpha
        self._beta = beta
        self._gamma = gamma

    @property
    def alpha(self):
        return self._alpha

    @alpha.setter
    def alpha(self, v):
        self._alpha = v


class TestSerializationRules(unittest.TestCase):

    def setUp(self):
        self.wr_rules = [(c, c.WR_RULE) for c in [RawTrivial, FancyTrivial]]
        self.wr_rules.append((int, ((b'', b'i'),)))
        self.cmd_rules = {
            (1, b'Obj1', b'iss'),
            (2, b'Obj2', b'sis')}

    def tearDown(self):
        pass

    def test_writable_rules(self):
        wr = sc.WritableRules()
        for k, v in self.wr_rules:
            wr.add(k, v)
        for k, v in self.wr_rules:
            vv = wr.rule(k)
            self.assertEqual(v, vv)
        self.assertEqual(wr.rule(type(None)), None)


def suite():
    return unittest.TestLoader().loadTestsFromTestCase(TestSerializationRules)


if __name__ == '__main__':
    _RUNNER = unittest.TextTestRunner(verbosity=2)
    _RUNNER.run((suite()))
