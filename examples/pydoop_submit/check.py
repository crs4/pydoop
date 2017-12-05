#!/usr/bin/env python

# BEGIN_COPYRIGHT
#
# Copyright 2009-2017 CRS4.
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

import pydoop.test_support as pts
import pydoop.hadut as hadut
import pydoop.hdfs as hdfs

THIS_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_INPUT = os.path.join(THIS_DIR, os.pardir, "input", "alice.txt")
CHECKS = [
    "nosep",
    "wordcount_minimal",
    "wordcount_full",
]


def check_wordcount_minimal(mr_out_dir):
    output = hadut.collect_output(mr_out_dir)
    local_wc = pts.LocalWordCount(DEFAULT_INPUT)
    res = local_wc.check(output)
    return res.startswith("OK")  # FIXME: change local_wc to raise an exception


check_wordcount_full = check_wordcount_minimal


def check_nosep(mr_out_dir):
    output = []
    for fn in hadut.iter_mr_out_files(mr_out_dir):
        with hdfs.open(fn, "rt") as f:
            for line in f:
                output.append(line.rstrip())
    with open(os.path.join(THIS_DIR, "data", "cols.txt")) as f:
        exp_output = ["".join(_.rstrip().split()) for _ in f]
    return sorted(exp_output) == sorted(output)


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
