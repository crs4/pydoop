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

import argparse
import io
import os
import sys

CHECKS = [
    "map_only_java_writer",
    "map_only_python_writer",
]


def get_lines(dir_path):
    rval = []
    for name in sorted(os.listdir(dir_path)):
        path = os.path.join(dir_path, name)
        if not os.path.isdir(path):
            with io.open(path, "rt") as f:
                for line in f:
                    rval.append(line.rstrip())
    return rval


def check_lines(lines, exp_lines):
    if len(lines) != len(exp_lines):
        raise RuntimeError("n. output lines = %d (expected: %d)" % (
            len(lines), len(exp_lines)
        ))
    for i, (ln, exp_ln) in enumerate(zip(lines, exp_lines)):
        if ln != exp_ln:
            raise RuntimeError("wrong output line #%d: %r (expected: %r)" % (
                i, ln, exp_ln
            ))


def check_map_only_java_writer(in_dir, out_dir):
    uc_lines = [_.upper() for _ in get_lines(in_dir)]
    out_values = [_.split("\t", 1)[1] for _ in get_lines(out_dir)]
    check_lines(out_values, uc_lines)


check_map_only_python_writer = check_map_only_java_writer


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("name", metavar="NAME", choices=CHECKS,
                        help="one of: %s" % "; ".join(CHECKS))
    parser.add_argument("mr_in", metavar="IN_DIR", help="MapReduce in dir")
    parser.add_argument("mr_out", metavar="OUT_DIR", help="MapReduce out dir")
    args = parser.parse_args(sys.argv[1:])
    check = globals()["check_%s" % args.name]
    check(args.mr_in, args.mr_out)
    sys.stdout.write("OK\n")
