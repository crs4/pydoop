from pydoop.app.submit import (
    add_parser_common_arguments,
    add_parser_arguments)
from pydoop.app.submit import PydoopSubmitter
import pydoop.hdfs as hdfs

import sys
import os
import argparse


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


main(sys.argv)
