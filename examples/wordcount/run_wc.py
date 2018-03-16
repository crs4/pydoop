#!/usr/bin/env python

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
import logging

logging.basicConfig(level=logging.INFO)

import pydoop
import pydoop.hadut as hadut
import pydoop.test_support as pts


CONF = {
    "mapred.map.tasks": "2",
    "mapred.reduce.tasks": "2",
    "mapred.job.name": "wordcount",
}
HADOOP_CONF_DIR = pydoop.hadoop_conf()
PREFIX = os.getenv("PREFIX", pts.get_wd_prefix())


def update_conf(args):
    if args.D:
        for kv_pair in args.D:
            k, v = [_.strip() for _ in kv_pair.split("=")]
            CONF[k] = v


def make_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument("pipes_exe", metavar="PIPES_EXE",
                        help="python script to be run by pipes")
    parser.add_argument("local_input", metavar="INPUT_DIR",
                        help="local input directory")
    parser.add_argument("-D", metavar="NAME=VALUE", action="append",
                        help="additional Hadoop configuration parameters")
    return parser


def main(argv):
    parser = make_parser()
    args = parser.parse_args(argv)
    update_conf(args)
    logger = logging.getLogger("main")
    logger.setLevel(logging.INFO)
    runner = hadut.PipesRunner(prefix=PREFIX, logger=logger)
    with open(args.pipes_exe) as f:
        pipes_code = pts.adapt_script(f.read())
    runner.set_input(args.local_input, put=True)
    runner.set_exe(pipes_code)
    runner.run(properties=CONF, hadoop_conf_dir=HADOOP_CONF_DIR, logger=logger)
    res = runner.collect_output()
    if not os.getenv("DEBUG"):
        runner.clean()
    local_wc = pts.LocalWordCount(args.local_input)
    logging.info(local_wc.check(res))


if __name__ == "__main__":
    main(sys.argv[1:])
