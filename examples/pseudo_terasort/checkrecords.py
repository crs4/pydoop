from pydoop.app.submit import (
    add_parser_common_arguments,
    add_parser_arguments)
from pydoop.app.submit import PydoopSubmitter

import sys
import argparse

def make_parser():
    parser = argparse.ArgumentParser()
    add_parser_common_arguments(parser)
    add_parser_arguments(parser)
    return parser


def main(argv=None):
    parser = make_parser()
    args, unknown_args = parser.parse_known_args(argv)
    args.job_name = 'pteracheck'
    args.module = 'pteracheck'
    args.do_not_use_java_record_reader = True
    args.do_not_use_java_record_writer = False
    args.num_reducers = 1
    args.upload_file_to_cache = ['pteracheck.py', 'ioformats.py']
    submitter = PydoopSubmitter()
    submitter.set_args(args, [] if unknown_args is None else unknown_args)
    submitter.run()


main(sys.argv)
