#!/usr/bin/env python

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
from ast import literal_eval

import pydoop.test_support as pts
import pydoop.hadut as hadut
import pydoop.hdfs as hdfs

THIS_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_INPUT_DIR = os.path.join(THIS_DIR, os.pardir, "input")
CHECKS = [
    "nosep",
    "wordcount_minimal",
    "wordcount_full",
    "map_only_java_writer",
    "map_only_python_writer",
]


def check_wordcount_minimal(mr_out_dir):
    output = hadut.collect_output(mr_out_dir)
    local_wc = pts.LocalWordCount(DEFAULT_INPUT_DIR)
    res = local_wc.check(output)
    return res.startswith("OK")  # FIXME: change local_wc to raise an exception


check_wordcount_full = check_wordcount_minimal


def check_nosep(mr_out_dir):
    output = []
    for fn in hadut.iter_mr_out_files(mr_out_dir):
        with hdfs.open(fn, "rt") as f:
            for line in f:
                output.append(line.rstrip())
    exp_output = []
    in_dir = os.path.join(THIS_DIR, "data")
    for name in os.listdir(in_dir):
        with open(os.path.join(in_dir, name)) as f:
            exp_output.extend(["".join(_.rstrip().split()) for _ in f])
    return sorted(exp_output) == sorted(output)


def check_map_only_python_writer(mr_out_dir):
    output = []
    for fn in hadut.iter_mr_out_files(mr_out_dir):
        with hdfs.open(fn, "rt") as f:
            for line in f:
                try:
                    t, rec = line.rstrip().split("\t", 1)
                except ValueError:
                    t, rec = line.rstrip(), ""
                output.append((literal_eval(t), rec))
    output = [_[1] for _ in sorted(output)]
    exp_output = []
    for name in sorted(os.listdir(DEFAULT_INPUT_DIR)):
        with open(os.path.join(DEFAULT_INPUT_DIR, name)) as f:
            exp_output.extend([_.rstrip().upper() for _ in f])
    return exp_output == output


check_map_only_java_writer = check_map_only_python_writer


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("name", metavar="NAME", choices=CHECKS,
                        help="one of: %s" % "; ".join(CHECKS))
    parser.add_argument("mr_out", metavar="DIR", help="MapReduce out dir")
    args = parser.parse_args(sys.argv[1:])
    check = globals()["check_%s" % args.name]
    if check(args.mr_out):
        print("OK.")
    else:
        sys.exit("ERROR: output differs from the expected one")
