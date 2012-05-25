# BEGIN_COPYRIGHT
# 
# Copyright 2012 CRS4.
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

import sys, argparse, importlib


SUBMOD_NAMES = [
  "script",
  ]


def make_parser():
  # Nothing fancy (e.g., subparsers) for now, we only have one command.
  parser = argparse.ArgumentParser(
    description="Pydoop command line tool",
    formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
  parser.add_argument('command', metavar="COMMAND", help='pydoop command',
                      choices=SUBMOD_NAMES)
  return parser


def main(argv=None):
  parser = make_parser()
  args, leftover_argv = parser.parse_known_args(argv)
  mod = importlib.import_module("%s.%s" % (__package__, args.command))
  sys.argv[0] = "pydoop %s" % args.command
  return mod.main(leftover_argv)
