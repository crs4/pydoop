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

from pydoop.app.submit import (
    add_parser_common_arguments,
    add_parser_arguments)
from pydoop.app.submit import PydoopSubmitter

import sys
import argparse

DEFAULT_NUM_RECORDS = 100000
DEFAULT_NUM_MAPS = 2
NUM_ROWS_KEY = 'mapreduce.pterasort.num-rows'
NUM_MAPS_KEY = 'mapreduce.job.maps'


def make_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--num-maps', metavar='INT', type=int,
        default=DEFAULT_NUM_MAPS,
        help=''
    )
    parser.add_argument(
        '--num-records', metavar='INT', type=int,
        default=DEFAULT_NUM_RECORDS,
        help=''
    )
    add_parser_common_arguments(parser)
    add_parser_arguments(parser)
    return parser


def add_D_arg(args, arg_name, arg_key):
    val = str(getattr(args, arg_name))
    if args.D is None:
        args.D = [[arg_key, val]]
    elif not any(map(lambda _: _[0] == arg_key,
                 args.D)):
        args.D.append([arg_key, val])


def main(argv=None):
    parser = make_parser()
    args, unknown_args = parser.parse_known_args(argv)
    args.job_name = 'pteragen'
    args.module = 'pteragen'
    args.upload_file_to_cache = ['pteragen.py', 'ioformats.py']
    args.input_format = 'it.crs4.pydoop.examples.pterasort.RangeInputFormat'
    args.do_not_use_java_record_writer = True
    # args.libjars = ['pydoop-input-formats.jar']
    add_D_arg(args, 'num_records', NUM_ROWS_KEY)
    add_D_arg(args, 'num_maps', NUM_MAPS_KEY)
    args.num_reducers = 0
    submitter = PydoopSubmitter()
    submitter.set_args(args, [] if unknown_args is None else unknown_args)
    submitter.run()


if __name__ == "__main__":
    main(sys.argv)
