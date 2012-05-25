#!/usr/bin/env python

# BEGIN_COPYRIGHT
# 
# Copyright 2012 CRS4.
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

import sys, argparse, logging
logging.basicConfig(level=logging.INFO)

import pydoop.test_support as pts
import pydoop.hdfs as hdfs
import pydoop.hadut as hadut


CONF = {
  "mapred.map.tasks": "2",
  "mapred.reduce.tasks": "2",
  "mapred.job.name": "wordcount",
  }


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
  exe, input_, output = [pts.make_random_str() for _ in xrange(3)]
  with open(args.pipes_exe) as f:
    pipes_code = pts.add_sys_path(f.read())
  logging.info("copying data to HDFS")
  hdfs.dump(pipes_code, exe)
  hdfs.put(args.local_input, input_)
  logging.info("running MapReduce application")
  hadut.run_pipes(exe, input_, output, properties=CONF)
  logging.info("checking results")
  res = pts.collect_output(output)
  for d in exe, input_, output:
    hdfs.rmr(d)
  local_wc = pts.LocalWordCount(args.local_input)
  logging.info(local_wc.check(res))


if __name__ == "__main__":
  main(sys.argv[1:])
