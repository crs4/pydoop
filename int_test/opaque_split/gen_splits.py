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
import sys

import pydoop.hdfs as hdfs
from pydoop.utils.serialize import OpaqueInputSplit, write_opaques


N_TASKS = 2
ITEMS_PER_TASK = 5


def gen_ranges():
    for i in range(N_TASKS):
        start = ITEMS_PER_TASK * i
        yield start, start + ITEMS_PER_TASK


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("splits_path", metavar="HDFS_PATH")
    args = parser.parse_args(sys.argv[1:])
    with hdfs.open(args.splits_path, "wb") as f:
        splits = [OpaqueInputSplit(1, _) for _ in gen_ranges()]
        write_opaques(splits, f)
