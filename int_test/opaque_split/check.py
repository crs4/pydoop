#!/usr/bin/env python

# BEGIN_COPYRIGHT
#
# Copyright 2009-2024 CRS4.
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

from gen_splits import N_TASKS, ITEMS_PER_TASK


def check_output(mr_out_dir):
    names = [_ for _ in os.listdir(mr_out_dir) if not _.startswith("_")]
    if len(names) != N_TASKS:
        raise RuntimeError("found %d output files (expected: %d)" %
                           (len(names), N_TASKS))
    idx = []
    for n in names:
        path = os.path.join(mr_out_dir, n)
        with io.open(path, "rt") as f:
            lines = [_.rstrip() for _ in f]
        if len(lines) != ITEMS_PER_TASK:
            raise RuntimeError("%s has %d lines (expected: %d)" %
                               (n, len(lines), ITEMS_PER_TASK))
        idx.extend(int(_.split("\t")[0]) for _ in lines)
    idx.sort()  # not sure order is guaranteed in a map-only job
    nitems = N_TASKS * ITEMS_PER_TASK
    if idx != list(range(nitems)):
        raise RuntimeError("overall indices != range(%d)" % nitems)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("mr_out", metavar="OUT_DIR", help="MapReduce out dir")
    args = parser.parse_args(sys.argv[1:])
    check_output(args.mr_out)
    sys.stdout.write("OK\n")
