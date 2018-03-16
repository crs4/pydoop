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

from __future__ import print_function

import sys
import os
import argparse
import logging

logging.basicConfig(level=logging.INFO)

import pydoop
import pydoop.hadut as hadut
import pydoop.hdfs as hdfs
import pydoop.test_support as pts
from timer import Timer


DEFAULT_SCRIPT = "../../examples/wordcount/bin/wordcount-full.py"

CONF = {
    "mapred.map.tasks": "2",
    "mapred.reduce.tasks": "2",
    "mapred.job.name": "wordcount",
    "hadoop.pipes.java.recordreader": "false",
    "hadoop.pipes.java.recordwriter": "false"
}

DATASET_DIR = "dataset"
HADOOP_CONF_DIR = pydoop.hadoop_conf()

PREFIX = os.getenv("PREFIX", pts.get_wd_prefix())
LOCAL_FILE_PREFIX = "file:/"
HDFS_FILE_PREFIX = "hdfs:///"


def update_conf(args):
    if args.D:
        for kv_pair in args.D:
            k, v = [_.strip() for _ in kv_pair.split("=")]
            CONF[k] = v
    if args.mappers:
        CONF["mapred.map.tasks"] = args.mappers
    if args.reducers:
        CONF["mapred.reduce.tasks"] = args.reducers


def make_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument("--script", metavar="script",
                        help="the script to launch")
    parser.add_argument("--mappers", metavar="# mappers", type=int,
                        help="the number of mappers for your mapred job")
    parser.add_argument("--reducers", metavar="# reducers", type=int,
                        help="number of reducers of your mapred job")
    parser.add_argument("--dataset", metavar="max file size for the dataset",
                        help="set to generate the dataset", type=int)
    parser.add_argument("-D", metavar="NAME=VALUE", action="append",
                        help="additional Hadoop configuration parameters")
    return parser


def create_dataset(logger, max_file_size_in_mb=200):

    logger.info("Creating the dataset")

    INPUT_FILE = "../../examples/input/alice.txt"
    if not os.path.exists(INPUT_FILE):
        raise IOError("input file not found")

    with open(INPUT_FILE) as f:
        text = f.read()

    base_text_file_length = len(text)

    if not os.path.exists("dataset"):
        os.mkdir("dataset")

    step_factor = 2

    step_file_length = 0
    step_file_length_mb = 0
    while step_file_length_mb < max_file_size_in_mb:
        step_file_length = (step_file_length
                            if step_file_length > 0
                            else base_text_file_length) * step_factor
        step_file_length_mb = int(step_file_length / 1048576)

        if step_file_length_mb == 0:
            continue

        filename = "dataset/{0}MB".format(step_file_length_mb)

        logger.info(" ->generating: %s", filename)
        with open(filename, "w") as f:

            file_length = 0

            while file_length < step_file_length:
                f.write(text)
                file_length += base_text_file_length


def main(argv):

    logger = logging.getLogger("main")
    logger.setLevel(logging.DEBUG)

    with Timer() as total_time:

        parser = make_parser()
        args = parser.parse_args(argv)
        if args.dataset:
            print(args.dataset)
            create_dataset(logger, args.dataset)

        if args.script:
            piped_code_file = args.script
        else:
            piped_code_file = DEFAULT_SCRIPT

        if not os.path.exists(piped_code_file):
            raise IOError("script {0} not found !!!".format(piped_code_file))

        with open(piped_code_file) as f:
            pipes_code = pts.adapt_script(f.read())

        dataset = [d for d in os.listdir("dataset") if d.endswith("MB")]
        dataset.sort(key=lambda x: int(x.replace("MB", "")))

        logger.info(" Uploading dataset: { %s }", ', '.join(dataset))
        if not hadut.path_exists(os.path.join(DATASET_DIR)):
            logger.info("  dataset folder created")
            hdfs.mkdir(DATASET_DIR)

        for data_filename in dataset:
            source_path = os.path.join(DATASET_DIR, data_filename)
            dest_path = os.path.join(DATASET_DIR, data_filename)

            if not hadut.path_exists(os.path.join(DATASET_DIR, data_filename)):
                logger.info(" -> uploading %s...", source_path)
                hdfs.put(source_path, dest_path)

        update_conf(args)

        results = dict()
        for data_input in dataset:

            with Timer() as t:
                runner = hadut.PipesRunner(prefix=PREFIX, logger=logger)
                logger.info("Running the script %s with data input %s..",
                            piped_code_file, data_input)
                data_input_path = os.path.join(DATASET_DIR, data_input)
                runner.set_input(data_input_path, put=False)
                runner.set_exe(pipes_code)
                runner.run(properties=CONF, hadoop_conf_dir=HADOOP_CONF_DIR,
                           logger=logger)
                res = runner.collect_output()
                print(data_input_path)
                local_wc = pts.LocalWordCount(data_input_path)
                logging.info(local_wc.check(res))
                # print(res)
                # runner.clean()
            results[data_input] = (t.secs, t.msecs)

    print("\n\n RESULTs")
    print("=" * (len(piped_code_file) + 15))
    print(" *  script: {0}".format(piped_code_file))
    print(" *  mappers: {0}".format(CONF["mapred.map.tasks"]))
    print(" *  reducers: {0}".format(CONF["mapred.reduce.tasks"]))
    print(" *  dataset: [{0}]".format(",".join(dataset)))
    print(" *  times (input -> secs):")
    for data_input in dataset:
        print("    - {0} -> {1} secs.".format(
            data_input, results[data_input][0]
        ))
    print("\n => Total execution time: {0}".format(total_time.secs))
    print("=" * (len(piped_code_file) + 15))
    print("\n")


if __name__ == "__main__":
    main(sys.argv[1:])
