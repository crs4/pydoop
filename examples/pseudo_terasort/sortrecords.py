from pterasort import Partitioner
from pydoop.app.submit import (
    add_parser_common_arguments,
    add_parser_arguments)
from pydoop.app.submit import PydoopSubmitter

import sys
import argparse

DEFAULT_SAMPLED_RECORDS = 100
DEFAULT_NUM_THREADS = 4


def make_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--sampled-records', metavar='INT', type=int,
        default=DEFAULT_SAMPLED_RECORDS,
        help=''
    )
    parser.add_argument(
        '--num-threads', metavar='INT', type=int,
        default=DEFAULT_NUM_THREADS,
        help=''
    )
    add_parser_common_arguments(parser)
    add_parser_arguments(parser)
    return parser


def main(argv=None):
    parser = make_parser()
    args, unknown_args = parser.parse_known_args(argv)
    args.job_name = 'pterasort'
    args.module = 'pterasort'
    args.do_not_use_java_record_reader = True
    args.do_not_use_java_record_writer = True
    bp_filename = Partitioner.initialize_break_points(args.num_reducers,
                                                      args.sampled_records,
                                                      args.input,
                                                      args.num_threads)
    args.upload_file_to_cache = ['pterasort.py', 'ioformats.py', bp_filename]
    submitter = PydoopSubmitter()
    submitter.set_args(args, [] if unknown_args is None else unknown_args)
    submitter.run()


main(sys.argv)
