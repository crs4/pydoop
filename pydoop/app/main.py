# BEGIN_COPYRIGHT
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
