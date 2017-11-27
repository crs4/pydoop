from pydoop.app.submit import (
    add_parser_common_arguments,
    add_parser_arguments)
from pydoop.app.submit import PydoopSubmitter

import sys
import argparse

DEFAULT_NUM_RECORDS = 100000
NUM_ROWS_KEY = 'mapreduce.pterasort.num-rows'
def make_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--num-records', metavar='INT', type=int,
        default=DEFAULT_NUM_RECORDS,
        help=''
    )
    add_parser_common_arguments(parser)
    add_parser_arguments(parser)
    return parser


def main(argv=None):
    parser = make_parser()
    args, unknown_args = parser.parse_known_args(argv)
    args.job_name = 'pteragen'
    args.module = 'pteragen'
    args.upload_file_to_cache = ['pteragen.py', 'ioformats.py']
    args.input_format = 'it.crs4.pydoop.examples.pterasort.RangeInputFormat'
    args.do_not_use_java_record_writer = True 
    
    # args.libjars = ['pydoop-input-formats.jar']
    if args.D is None:
        args.D = {NUM_ROWS_KEY: str(args.num_records)}
    elif any(map(lambda _: _.startswith(NUM_ROWS_KEY),
             args.D)):
        args.D.append(NUM_ROWS_KEY, str(args.num_records))
    args.num_reducers = 0
    submitter = PydoopSubmitter()
    submitter.set_args(args, [] if unknown_args is None else unknown_args)
    submitter.run()

main(sys.argv)
