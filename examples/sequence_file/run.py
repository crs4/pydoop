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

"""
Run a word count on the input, storing counts as 32-bit integers in
Hadoop SequenceFiles; subsequently, run a MapReduce application that
filters out those words whose count falls below a specified threshold.

The purpose of this example is to demonstrate the usage of
SequenceFileInputFormat and SequenceFileOutputFormat.
"""

import os
import optparse
import logging

logging.basicConfig(level=logging.INFO)

import pydoop
import pydoop.test_support as pts
import pydoop.hadut as hadut


HADOOP = pydoop.hadoop_exec()
HADOOP_CONF_DIR = pydoop.hadoop_conf()
OUTPUT = "output"
LOCAL_WC_SCRIPT = "bin/wordcount.py"
LOCAL_FILTER_SCRIPT = "bin/filter.py"

THIS_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_INPUT = os.path.normpath(os.path.join(THIS_DIR, "../input"))

MR_JOB_NAME = "mapred.job.name"
MR_HOME_DIR = 'mapreduce.admin.user.home.dir'
PIPES_JAVA_RR = "hadoop.pipes.java.recordreader"
PIPES_JAVA_RW = "hadoop.pipes.java.recordwriter"
MR_OUT_COMPRESS_TYPE = "mapred.output.compression.type"
MR_REDUCE_TASKS = "mapred.reduce.tasks"
MR_IN_CLASS = "mapred.input.format.class"
MR_OUT_CLASS = "mapred.output.format.class"
MRLIB = "org.apache.hadoop.mapred"

BASE_MR_OPTIONS = {
    PIPES_JAVA_RR: "true",
    PIPES_JAVA_RW: "true",
    MR_HOME_DIR: os.path.expanduser("~"),
}

PREFIX = os.getenv("PREFIX", pts.get_wd_prefix())


def make_parser():
    parser = optparse.OptionParser(usage="%prog [OPTIONS]")
    parser.add_option("-i", dest="input", metavar="STRING",
                      help="input dir/file ['%default']",
                      default=DEFAULT_INPUT)
    parser.add_option("-t", type="int", dest="threshold", metavar="INT",
                      help="min word occurrence [%default]", default=10)
    return parser


def run_wc(opt):
    runner = hadut.PipesRunner(prefix=PREFIX)
    options = BASE_MR_OPTIONS.copy()
    options.update({
        MR_JOB_NAME: "wordcount",
        MR_OUT_CLASS: "%s.SequenceFileOutputFormat" % MRLIB,
        MR_OUT_COMPRESS_TYPE: "NONE",
        MR_REDUCE_TASKS: "2",
    })
    with open(LOCAL_WC_SCRIPT) as f:
        pipes_code = pts.adapt_script(f.read())
    runner.set_input(opt.input, put=True)
    runner.set_exe(pipes_code)
    runner.run(properties=options, hadoop_conf_dir=HADOOP_CONF_DIR)
    return runner.output


def run_filter(opt, input_):
    runner = hadut.PipesRunner(prefix=PREFIX)
    options = BASE_MR_OPTIONS.copy()
    options.update({
        MR_JOB_NAME: "filter",
        MR_IN_CLASS: "%s.SequenceFileInputFormat" % MRLIB,
        MR_REDUCE_TASKS: "0",
        "filter.occurrence.threshold": opt.threshold,
    })
    with open(LOCAL_FILTER_SCRIPT) as f:
        pipes_code = pts.adapt_script(f.read())
    runner.set_input(input_)
    runner.set_exe(pipes_code)
    runner.run(properties=options, hadoop_conf_dir=HADOOP_CONF_DIR)
    return runner.output


def main():
    parser = make_parser()
    opt, _ = parser.parse_args()
    logger = logging.getLogger("main")
    logger.setLevel(logging.INFO)
    logger.info("running word count")
    wc_output = run_wc(opt)
    logger.info("running filter")
    filter_output = run_filter(opt, wc_output)
    logger.info("checking results")
    res = hadut.collect_output(filter_output)
    local_wc = pts.LocalWordCount(opt.input, min_occurrence=opt.threshold)
    logger.info(local_wc.check(res))


if __name__ == "__main__":
    main()
