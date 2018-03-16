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

import unittest
import shutil
import tempfile
import os

import pydoop.app.main as app
from pydoop.app.submit import PydoopSubmitter
import re


def nop(x=None):
    pass


class Args(object):
    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)

    def __getattr__(self, _):
        """
        If we don't have the requested attribute return None.
        """
        return None


class TestAppSubmit(unittest.TestCase):

    def setUp(self):
        self.submitter = PydoopSubmitter()

    @staticmethod
    def _gen_default_args():
        return Args(
            entry_point='__main__',
            log_level='INFO',
            module='the_module',
            no_override_env=False,
            no_override_home=False,
            python_program='python',
            output="output_path",
            job_name="job_name",
            num_reducers=0,
        )

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
            self.assertEqual(e.args[0], 0)
        try:
            args, unk = parser.parse_known_args(['submit', '-h'])
        except SystemExit as e:
            self.assertEqual(e.args[0], 0)
        try:
            args, unk = parser.parse_known_args(['submit'])
        except SystemExit as e:
            self.assertEqual(e.args[0], 2)

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

    def test_generate_pipes_code_env(self):
        args = self._gen_default_args()
        self.submitter.set_args(args)
        old_ld_lib_path = os.environ.get('LD_LIBRARY_PATH', '')

        try:
            # we set this variable for this test since it may not be set in
            # the environment
            os.environ['LD_LIBRARY_PATH'] = '/test_path'
            code = self.submitter._generate_pipes_code()
            self.assertTrue('export PATH=' in code)
            self.assertTrue('export PYTHONPATH=' in code)
            self.assertTrue('export LD_LIBRARY_PATH="/test_path"' in code)
        finally:
            os.environ['LD_LIBRARY_PATH'] = old_ld_lib_path

    def test_generate_pipes_code_no_override_ld_path(self):
        args = self._gen_default_args()
        args.no_override_ld_path = True
        self.submitter.set_args(args)
        old_ld_lib_path = os.environ.get('LD_LIBRARY_PATH', '')

        try:
            os.environ['LD_LIBRARY_PATH'] = '/test_path'
            code = self.submitter._generate_pipes_code()
            self.assertTrue('export PYTHONPATH=' in code)
            self.assertFalse('export LD_LIBRARY_PATH=' in code)
        finally:
            os.environ['LD_LIBRARY_PATH'] = old_ld_lib_path

    def test_generate_pipes_code_no_override_path(self):
        args = self._gen_default_args()
        args.no_override_path = True
        self.submitter.set_args(args)

        code = self.submitter._generate_pipes_code()
        self.assertTrue('export PYTHONPATH=' in code)
        self.assertFalse('export PATH=' in code)

    def test_generate_pipes_code_no_override_pythonpath(self):
        args = self._gen_default_args()
        args.no_override_pypath = True
        self.submitter.set_args(args)

        code = self.submitter._generate_pipes_code()
        self.assertTrue('export PYTHONPATH="${PWD}:${PYTHONPATH}"' in code)
        self.assertTrue('export PATH=' in code)

    def test_generate_pipes_code_with_set_env(self):
        args = self._gen_default_args()
        args.set_env = ["PATH=/my/custom/path"]
        self.submitter.set_args(args)
        old_ld_lib_path = os.environ.get('LD_LIBRARY_PATH', '')

        try:
            os.environ['LD_LIBRARY_PATH'] = '/test_path'
            code = self.submitter._generate_pipes_code()
            self.assertTrue('export PATH="/my/custom/path"' in code)
            self.assertTrue('export PYTHONPATH=' in code)
            self.assertTrue('export LD_LIBRARY_PATH="/test_path"' in code)
        finally:
            os.environ['LD_LIBRARY_PATH'] = old_ld_lib_path

    def test_generate_code_no_env_override(self):
        args = self._gen_default_args()
        args.no_override_env = True
        self.submitter.set_args(args)

        code = self.submitter._generate_pipes_code()
        self.assertFalse('export PATH=' in code)
        self.assertFalse('export LD_LIBRARY_PATH="/test_path"' in code)
        # PYTHONPATH should still be there because we add the hadoop
        # working directory
        self.assertTrue('export PYTHONPATH=' in code)

    def test_generate_code_no_env_override_with_set_env(self):
        args = self._gen_default_args()
        args.no_override_env = True
        args.set_env = ["PATH=/my/custom/path"]
        self.submitter.set_args(args)

        code = self.submitter._generate_pipes_code()

        self.assertTrue('export PATH="/my/custom/path"' in code)
        self.assertFalse('export LD_LIBRARY_PATH="/test_path"' in code)
        # PYTHONPATH should still be there because we add the hadoop
        # working directory
        self.assertTrue('export PYTHONPATH=' in code)

    def test_env_arg_to_dict(self):
        env_arg = ['var1=value1', ' var2 = value2 ', 'var3 = str with = sign']
        d = self.submitter._env_arg_to_dict(env_arg)
        self.assertEquals('value1', d['var1'])
        self.assertEquals('value2', d['var2'])
        self.assertEquals('str with = sign', d['var3'])


def suite():
    suite_ = unittest.TestLoader().loadTestsFromTestCase(TestAppSubmit)
    return suite_


if __name__ == '__main__':
    _RUNNER = unittest.TextTestRunner(verbosity=2)
    _RUNNER.run((suite()))
