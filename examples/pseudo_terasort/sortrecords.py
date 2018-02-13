# BEGIN_COPYRIGHT
#
# Copyright 2009-2018 CRS4.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy
# of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.
#
# END_COPYRIGHT

import sys
import argparse

from pydoop.app.submit import (
    add_parser_common_arguments,
    add_parser_arguments)
from pydoop.app.submit import PydoopSubmitter
from pterasort import Partitioner

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


if __name__ == "__main__":
    main(sys.argv)
