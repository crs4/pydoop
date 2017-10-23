# BEGIN_COPYRIGHT
#
# Copyright 2009-2017 CRS4.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy
# of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
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
from collections import Counter

import pydoop.hadut as hadut

THIS_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_INPUT = os.path.join(THIS_DIR, os.pardir, "input", "alice.txt")
CHECKS = [
    "base_histogram",
    "caseswitch",
    "grep",
    "grep_compiled",
    "lowercase",
    "transpose",
]


def check_base_histogram(mr_out_dir):
    output = Counter()
    for line in hadut.collect_output(mr_out_dir).splitlines():
        k, v = line.split("\t")
        output[k] = int(v)
    exp_output = Counter()
    with open(os.path.join(THIS_DIR, "example.sam")) as f:
        for line in f:
            for base in line.rstrip().split("\t", 10)[9]:
                exp_output[base] += 1
    return output == exp_output


def check_caseswitch(mr_out_dir, switch="upper"):
    output = hadut.collect_output(mr_out_dir)
    with open(DEFAULT_INPUT) as f:
        exp_output = getattr(f.read(), "upper")()
    return output == exp_output


def check_grep(mr_out_dir):
    output = hadut.collect_output(mr_out_dir).splitlines()
    with open(DEFAULT_INPUT) as f:
        exp_output = [_.strip() for _ in f if "March" in _]
    return output == exp_output


check_grep_compiled = check_grep


def check_lowercase(mr_out_dir):
    return check_caseswitch(mr_out_dir, switch="lower")


def check_transpose(mr_out_dir):
    output = []
    for line in hadut.collect_output(mr_out_dir).splitlines():
        output.append(line.split("\t")[1:])  # skip initial row index
    exp_output = []
    with open(os.path.join(THIS_DIR, "matrix.txt")) as f:
        for line in f:
            for i, item in enumerate(line.split()):
                try:
                    exp_output[i].append(item)
                except IndexError:
                    exp_output.append([item])
    return output == exp_output


def make_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument("name", metavar="NAME", choices=CHECKS,
                        help="one of: %s" % "; ".join(CHECKS))
    parser.add_argument("mr_out", metavar="DIR", help="MapReduce out dir")
    return parser


def main(argv):
    parser = make_parser()
    args = parser.parse_args(argv)
    check = globals()["check_%s" % args.name]
    if check(args.mr_out):
        print("OK.")
    else:
        sys.exit("ERROR: output differs from the expected one")


if __name__ == "__main__":
    main(sys.argv[1:])
