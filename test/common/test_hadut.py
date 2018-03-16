# BEGIN_COPYRIGHT
#
# Copyright 2009-2018 CRS4.
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

# pylint: disable=W0212

"""
Test suite for pydoop.hadut
"""

import unittest

import pydoop.hadut as hadut


def pair_set(seq):
    return set((seq[i], seq[i + 1]) for i in range(0, len(seq), 2))


class TestHadut(unittest.TestCase):

    def assertEqualPairSet(self, seq1, seq2):
        return self.assertEqual(pair_set(seq1), pair_set(seq2))

    def test_pop_generic_args(self):
        self.assertRaises(ValueError, hadut._pop_generic_args, ['-fs'])
        args = [
            '-input', 'i',
            '-libjars', 'l',
            '-fs', 'f',
            '-output', 'o',
            '-jar', 'pippo'
        ]
        gargs = hadut._pop_generic_args(args)
        self.assertEqualPairSet(gargs, ['-libjars', 'l', '-fs', 'f'])
        self.assertEqualPairSet(
            args, ['-input', 'i', '-output', 'o', '-jar', 'pippo']
        )

    def test_merge_csv_args(self):
        self.assertRaises(ValueError, hadut._merge_csv_args, ['-archives'])
        args = [
            '-libjars', 'l1',
            '-fs', 'f',
            '-libjars', 'l2',
            '-files', 'pippo',
        ]
        hadut._merge_csv_args(args)
        try:
            self.assertEqualPairSet(
                args, ['-libjars', 'l1,l2', '-fs', 'f', '-files', 'pippo']
            )
        except AssertionError:
            self.assertEqualPairSet(
                args, ['-libjars', 'l2,l1', '-fs', 'f', '-files', 'pippo']
            )


def suite():
    suite_ = unittest.TestSuite()
    suite_.addTest(TestHadut('test_pop_generic_args'))
    suite_.addTest(TestHadut('test_merge_csv_args'))
    return suite_


if __name__ == '__main__':
    _RUNNER = unittest.TextTestRunner(verbosity=2)
    _RUNNER.run((suite()))
