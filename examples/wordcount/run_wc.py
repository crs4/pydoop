#!/usr/bin/env python

# BEGIN_COPYRIGHT
# END_COPYRIGHT

import sys, os, argparse, logging
logging.basicConfig(level=logging.INFO)

import pydoop.test_support as pts
import pydoop.hdfs as hdfs
import pydoop.hadut as hadut
import check_output


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
  logging.info(check_output.check(args.local_input, res))


if __name__ == "__main__":
  main(sys.argv[1:])
