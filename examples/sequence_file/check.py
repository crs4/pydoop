#!/usr/bin/env python

# BEGIN_COPYRIGHT
#
# Copyright 2009-2019 CRS4.
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

import argparse

import pydoop.test_support as pts
import pydoop.hadut as hadut


def main(args):
    output = hadut.collect_output(args.output)
    local_wc = pts.LocalWordCount(args.input, min_occurrence=args.threshold)
    res = local_wc.check(output)
    if res.startswith("OK"):  # FIXME: change local_wc to raise an exception
        print("OK.")
    else:
        raise RuntimeError("output differs from the expected one")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("input", metavar="INPUT_DIR")
    parser.add_argument("output", metavar="OUTPUT_DIR")
    parser.add_argument("-t", "--threshold", type=int, metavar="INT",
                        help="min word occurrence", default=10)
    main(parser.parse_args())
