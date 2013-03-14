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

"""
Pydoop command line tool.
"""

import argparse, importlib

from pydoop.version import version

SUBMOD_NAMES = [
  "script",
  ]

def make_parser():
  parser = argparse.ArgumentParser(
    description="Pydoop command line tool",
    formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
  parser.add_argument('-V', '--version', action='version', version=version,
                      help='print version number and exit')
  subparsers = parser.add_subparsers(help="sub-commands")
  for n in SUBMOD_NAMES:
    mod = importlib.import_module("%s.%s" % (__package__, n))
    mod.add_parser(subparsers)
  return parser


def main(argv=None):
  parser = make_parser()
  args = parser.parse_args(argv)
  args.func(args)
