# BEGIN_COPYRIGHT
#
# Copyright 2009-2018 CRS4.
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
import pydoop.hdfs as hdfs
import pydoop.test_support as pts

THIS_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_INPUT_DIR = os.path.join(THIS_DIR, os.pardir, "input")
CHECKS = [
    "base_histogram",
    "caseswitch",
    "grep",
    "grep_compiled",
    "lowercase",
    "transpose",
    "wc_combiner",
    "wordcount",
    "wordcount_sw",
]


def check_base_histogram(mr_out_dir):
    output = Counter()
    for line in hadut.collect_output(mr_out_dir).splitlines():
        k, v = line.split("\t")
        output[k] = int(v)
    exp_output = Counter()
    in_dir = os.path.join(THIS_DIR, "data", "base_histogram_input")
    for name in os.listdir(in_dir):
        with open(os.path.join(in_dir, name)) as f:
            for line in f:
                for base in line.rstrip().split("\t", 10)[9]:
                    exp_output[base] += 1
    return output == exp_output


def check_caseswitch(mr_out_dir, switch="upper"):
    output = hadut.collect_output(mr_out_dir)
    exp_output = []
    for name in sorted(os.listdir(DEFAULT_INPUT_DIR)):
        with open(os.path.join(DEFAULT_INPUT_DIR, name)) as f:
            exp_output.append(getattr(f.read(), switch)())
    exp_output = "".join(exp_output)
    return output.splitlines() == exp_output.splitlines()


def check_grep(mr_out_dir):
    output = hadut.collect_output(mr_out_dir).splitlines()
    exp_output = []
    for name in sorted(os.listdir(DEFAULT_INPUT_DIR)):
        with open(os.path.join(DEFAULT_INPUT_DIR, name)) as f:
            exp_output.extend([_.strip() for _ in f if "March" in _])
    return output == exp_output


check_grep_compiled = check_grep


def check_lowercase(mr_out_dir):
    return check_caseswitch(mr_out_dir, switch="lower")


def check_transpose(mr_out_dir):
    output = []
    for fn in hadut.iter_mr_out_files(mr_out_dir):
        with hdfs.open(fn, "rt") as f:
            for line in f:
                row = line.rstrip().split("\t")
                index = int(row.pop(0))
                output.append((index, row))
    output = [_[1] for _ in sorted(output)]
    exp_output = []
    in_fn = os.path.join(THIS_DIR, "data", "transpose_input", "matrix.txt")
    with open(in_fn) as f:
        for line in f:
            for i, item in enumerate(line.split()):
                try:
                    exp_output[i].append(item)
                except IndexError:
                    exp_output.append([item])
    return output == exp_output


def check_wordcount(mr_out_dir, stop_words=None):
    output = hadut.collect_output(mr_out_dir)
    local_wc = pts.LocalWordCount(DEFAULT_INPUT_DIR, stop_words=stop_words)
    res = local_wc.check(output)
    return res.startswith("OK")  # FIXME: change local_wc to raise an exception


def check_wordcount_sw(mr_out_dir):
    with open(os.path.join(THIS_DIR, "data", "stop_words.txt"), "rt") as f:
        stop_words = frozenset(_.strip() for _ in f if not _.isspace())
    return check_wordcount(mr_out_dir, stop_words=stop_words)


check_wc_combiner = check_wordcount


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
