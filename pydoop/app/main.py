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

"""
Pydoop command line tool.
"""

import os
import argparse
import importlib
import sys


from pydoop.version import version

SUBMOD_NAMES = [
    "script",
    "submit",
]

PYDOOP_CONF_FILE = "~/.pydoop/pydoop.conf"


class PatchedArgumentParser(argparse.ArgumentParser):
    """
    This is a work-around for a bug in ArgumentParser that is triggered
    when there is a zero length argument and fromfile_prefix_chars is
    not None.
    """
    def _read_args_from_files(self, arg_strings):
        place_holder = "abcjdkje-32333a290"
        assert not (place_holder in arg_strings)
        args = [x if len(x) > 0 else place_holder for x in arg_strings]
        new_args = super(PatchedArgumentParser,
                         self)._read_args_from_files(args)
        return [x if x != place_holder else '' for x in new_args]


def make_parser():
    parser = PatchedArgumentParser(
        description="Pydoop command line tool",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        epilog=("Supports argparse @confile syntax "),
        fromfile_prefix_chars='@'
    )
    parser._pydoop_docs_helper = {}
    parser.add_argument('-V', '--version', action='version', version=version,
                        help='print version number and exit')
    subparsers = parser.add_subparsers(help="sub-commands")
    for n in SUBMOD_NAMES:
        mod = importlib.import_module("%s.%s" % (__package__, n))
        subp = mod.add_parser(subparsers)
        parser._pydoop_docs_helper[n] = subp
    return parser


def main(argv=None):
    parser = make_parser()
    if os.path.exists(PYDOOP_CONF_FILE):
        argv = argv + ['@' + PYDOOP_CONF_FILE]
    args, unknown = parser.parse_known_args(argv)
    try:
        if args.combiner_fn and not args.combine_fn:
            args.combine_fn = args.combiner_fn  # backwards compatibility
    except AttributeError:  # not the script app
        pass
    try:
        args.func(args, unknown)
    except AttributeError:
        parser.error("too few arguments")
    except RuntimeError as e:
        sys.exit("ERROR - {}:  {}".format(type(e).__name__, e))
