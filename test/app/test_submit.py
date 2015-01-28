# BEGIN_COPYRIGHT
#
# Copyright 2009-2015 CRS4.
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
import shutil
import tempfile
import os

import pydoop.app.main as app
import re


def nop(x=None):
    pass


class TestAppSubmit(unittest.TestCase):

    def test_help(self):
        parser = app.make_parser()
        # silence!
        for k in ['submit', 'script']:
            parser._actions[2].choices[k].format_help = nop
            parser._actions[2].choices[k].format_usage = nop
            parser._actions[2].choices[k].error = nop
        parser.format_help = nop
        parser.format_usage = nop
        parser.error = nop
        try:
            args, unk = parser.parse_known_args(['-h'])
        except SystemExit as e:
            self.assertEqual(e.message, 0)
        try:
            args, unk = parser.parse_known_args(['submit', '-h'])
        except SystemExit as e:
            self.assertEqual(e.message, 0)
        try:
            args, unk = parser.parse_known_args(['submit'])
        except SystemExit as e:
            self.assertEqual(e.message, 2)

    def _check_args(self, args, args_kv):
        for k, v in args_kv:
            k = re.sub("^--", "", k).replace('-', '_')
            self.assertTrue(hasattr(args, k))
            v1 = getattr(args, k)
            if v is None:
                self.assertEqual(v1, True)
            elif type(v1) is list:
                pass
            else:
                self.assertEqual(v1, v)

    def test_conf_file(self):
        wd = tempfile.mkdtemp(prefix='pydoop_')
        conf_file = os.path.join(wd, 'pydoop.conf')
        args_kv = (("--pretend", None),
                   ("--mrv2", None),
                   ("--input-format", 'mapreduce.lib.input.TextInputFormat'),
                   ("--output-format", 'mapreduce.lib.input.TextOutputFormat'),
                   ("--num-reducers", 10),
                   ("--python-zip", 'allmymodules.zip'),
                   )
        try:
            with open(conf_file, 'w') as cf:
                d = ''.join(['{}\n{}\n'.format(k, v)
                             if v is not None else '{}\n'.format(k)
                             for (k, v) in args_kv])
                cf.write(d)
            parser = app.make_parser()
            parser.format_help = nop
            module = 'mymod1.mod2.mod3'
            ainput = 'input'
            aoutput = 'output'
            argv = ['submit', module, ainput, aoutput, '@' + conf_file]
            [args, unknown] = parser.parse_known_args(argv)
            self.assertEqual(args.module, module)
            self.assertEqual(args.input, ainput)
            self.assertEqual(args.output, aoutput)
            self.assertEqual(len(unknown), 0)
            self._check_args(args, args_kv)
        finally:
            shutil.rmtree(wd)

    def test_empty_param(self):
        parser = app.make_parser()
        parser.format_help = nop
        program = 'program'
        ainput = 'input'
        aoutput = 'output'
        argv = ['submit', '--module', '', program, ainput, aoutput]
        [args, unknown] = parser.parse_known_args(argv)
        self.assertEqual(args.module, '')


def suite():
    suite_ = unittest.TestSuite()
    suite_.addTest(TestAppSubmit('test_help'))
    suite_.addTest(TestAppSubmit('test_conf_file'))
    suite_.addTest(TestAppSubmit('test_empty_param'))
    return suite_


if __name__ == '__main__':
    _RUNNER = unittest.TextTestRunner(verbosity=2)
    _RUNNER.run((suite()))
