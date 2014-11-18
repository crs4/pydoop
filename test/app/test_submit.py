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

import pydoop.app.main as app


class TestAppSubmit(unittest.TestCase):

    def test_help(self):
        try:
            app.main(['-h'])
        except SystemExit as e:
            self.assertEqual(e.message, 0)
        try:
            app.main(['submit', '-h'])
        except SystemExit as e:
            self.assertEqual(e.message, 0)
        try:
            app.main(['submit'])
        except SystemExit as e:
            self.assertEqual(e.message, 2)

    def test_pretend_submission(self):
        args_line = """
        submit
        --pretend --mrv2
        --input-format  mapreduce.lib.input.TextInputFormat
        --output-format mapreduce.lib.input.TextOutputFormat
        --num-reducers 10
        --python-egg allmymodules.egg
        --module mymod1.mod2.mod3
        my_program my_input my_output
        """
        with self.assertRaises(RuntimeError):
            app.main(args_line.split())

    def test_validate(self):
        pass

    def test_wrap(self):
        pass

    def test_conversion(self):
        pass


def suite():
    suite_ = unittest.TestSuite()
    suite_.addTest(TestAppSubmit('test_help'))
    suite_.addTest(TestAppSubmit('test_pretend_submission'))
    return suite_


if __name__ == '__main__':
    _RUNNER = unittest.TextTestRunner(verbosity=2)
    _RUNNER.run((suite()))
