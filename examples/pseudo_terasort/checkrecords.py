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
import os
import argparse

from pydoop.app.submit import (
    add_parser_common_arguments,
    add_parser_arguments)
from pydoop.app.submit import PydoopSubmitter
import pydoop.hdfs as hdfs


def make_parser():
    parser = argparse.ArgumentParser()
    add_parser_common_arguments(parser)
    add_parser_arguments(parser)
    return parser


def check_rows(rows):
    orbndry = None
    for r in rows:
        fname, bndrys = r.split(b'\t', 2)
        lbndry, rbndry = eval(bndrys)
        if orbndry is None:
            orbndry = rbndry
        else:
            assert orbndry <= lbndry
        print("{} [{}, {}]".format(fname, lbndry, rbndry))


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
    path = os.path.join(args.output, 'part-r-00000')
    with hdfs.open(path, 'rb') as f:
        data = f.read()
    check_rows(data.split(b'\n')[:-1])


if __name__ == "__main__":
    main(sys.argv)
