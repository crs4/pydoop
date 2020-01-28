#!/usr/bin/env python

# BEGIN_COPYRIGHT
#
# Copyright 2009-2020 CRS4.
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
import pstats
import sys
from collections import Counter
from itertools import chain


def get_lines(dir_path):
    rval = []
    for name in sorted(os.listdir(dir_path)):
        path = os.path.join(dir_path, name)
        if not os.path.isdir(path):
            with io.open(path, "rt") as f:
                for line in f:
                    rval.append(line.rstrip())
    return rval


def check_output(items, exp_items):
    if len(items) != len(exp_items):
        raise RuntimeError("n. output items = %d (expected: %d)" % (
            len(items), len(exp_items)
        ))
    for i, (it, exp_it) in enumerate(zip(items, exp_items)):
        if it != exp_it:
            raise RuntimeError("wrong output item #%d: %r (expected: %r)" % (
                i, it, exp_it
            ))


def check_counters(counter, exp_counter):
    return check_output(sorted(counter.items()), sorted(exp_counter.items()))


def word_count(lines):
    return Counter(chain(*(_.split() for _ in lines)))


def check_map_only(in_dir, out_dir):
    uc_lines = [_.upper() for _ in get_lines(in_dir)]
    out_values = [_.split("\t", 1)[1] for _ in get_lines(out_dir)]
    check_output(out_values, uc_lines)


def check_map_reduce(in_dir, out_dir):
    wc = word_count(get_lines(in_dir))
    out_pairs = (_.split("\t", 1) for _ in get_lines(out_dir))
    out_wc = {k: int(v) for k, v in out_pairs}
    check_counters(out_wc, wc)


def check_pstats(pstats_dir):
    pstats_names = os.listdir(pstats_dir)
    try:
        bn = pstats_names[0]
    except IndexError:
        raise RuntimeError("%r is empty" % (pstats_dir,))
    pstats.Stats(os.path.join(pstats_dir, bn))


CHECKS = {
    "map_only_java_writer": check_map_only,
    "map_only_python_writer": check_map_only,
    "map_reduce_combiner": check_map_reduce,
    "map_reduce_java_rw": check_map_reduce,
    "map_reduce_java_rw_pstats": check_map_reduce,
    "map_reduce_python_partitioner": check_map_reduce,
    "map_reduce_python_reader": check_map_reduce,
    "map_reduce_python_writer": check_map_reduce,
    "map_reduce_raw_io": check_map_reduce,
    "map_reduce_slow_java_rw": check_map_reduce,
    "map_reduce_slow_python_rw": check_map_reduce,
}


if __name__ == "__main__":
    choices = sorted(CHECKS)
    parser = argparse.ArgumentParser()
    parser.add_argument("name", metavar="NAME", choices=choices,
                        help="one of: %s" % "; ".join(choices))
    parser.add_argument("mr_in", metavar="IN_DIR", help="MapReduce in dir")
    parser.add_argument("mr_out", metavar="OUT_DIR", help="MapReduce out dir")
    args = parser.parse_args(sys.argv[1:])
    check = CHECKS[args.name]
    check(args.mr_in, args.mr_out)
    if "pstats" in args.name:
        check_pstats("%s.stats" % args.mr_out)
    sys.stdout.write("OK\n")
