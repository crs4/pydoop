#!/usr/bin/env python

# BEGIN_COPYRIGHT
# END_COPYRIGHT

import sys, os, optparse, subprocess as sp
import pydoop.hadut

HADOOP = pydoop.hadut.hadoop
WD = "test_sequence_file"
LOCAL_WC_SCRIPT = "bin/wordcount.py"
LOCAL_FILTER_SCRIPT = "bin/filter.py"

MR_JOB_NAME = "mapred.job.name"
MR_HOME_DIR = 'mapreduce.admin.user.home.dir'
PIPES_JAVA_RR = "hadoop.pipes.java.recordreader"
PIPES_JAVA_RW = "hadoop.pipes.java.recordwriter"
MR_OUT_COMPRESS_TYPE = "mapred.output.compression.type"
MR_REDUCE_TASKS = "mapred.reduce.tasks"
MR_IN_CLASS = "mapred.input.format.class"
MR_OUT_CLASS = "mapred.output.format.class"

BASE_MR_OPTIONS = {
  PIPES_JAVA_RR: "true",
  PIPES_JAVA_RW: "true",
  MR_HOME_DIR: os.path.expanduser("~"),
  }


def build_d_options(opt_dict):
  d_options = []
  for name, value in opt_dict.iteritems():
    d_options.append("-D %s=%s" % (name, value))
  return " ".join(d_options)


def hadoop_pipes(pipes_opts, hadoop=HADOOP):
  cmd = "%s pipes %s" % (hadoop, pipes_opts)
  print "running '%s'" % cmd
  p = sp.Popen(cmd, shell=True)
  return os.waitpid(p.pid, 0)[1]


def make_parser():
  parser = optparse.OptionParser(usage="%prog [OPTIONS] INPUT")
  parser.add_option("-t", type="int", dest="threshold", metavar="INT",
                    help="min word occurrence [%default]", default=10)
  return parser


def main(argv):

  parser = make_parser()
  opt, args = parser.parse_args()
  try:
    local_input = args[0]
  except IndexError:
    parser.print_help()
    sys.exit(2)

  mrlib = "org.apache.hadoop.mapred"
  wc_options = BASE_MR_OPTIONS.copy()
  wc_options.update({
    MR_JOB_NAME: "wordcount",
    MR_OUT_CLASS: "%s.SequenceFileOutputFormat" % mrlib,
    MR_OUT_COMPRESS_TYPE: "NONE",
    })
  filter_options = BASE_MR_OPTIONS.copy()
  filter_options.update({
    MR_JOB_NAME: "filter",
    MR_IN_CLASS: "%s.SequenceFileInputFormat" % mrlib,
    MR_REDUCE_TASKS: "0",
    "filter.occurrence.threshold": opt.threshold,
    })
  
  wc_script = "%s/%s" % (WD, LOCAL_WC_SCRIPT)
  filter_script = "%s/%s" % (WD, LOCAL_FILTER_SCRIPT)
  input_ = "%s/input" % WD
  wc_output = "%s/wc_output" % WD
  filter_output = "%s/filter_output" % WD

  sp.call("%s fs -rmr %s" % (HADOOP, WD), shell=True)
  sp.call("%s fs -mkdir %s/bin" % (HADOOP, WD), shell=True)
  for local, remote in [
    (local_input, input_),
    (LOCAL_WC_SCRIPT, wc_script),
    (LOCAL_FILTER_SCRIPT, filter_script),
    ]:
    sp.call("%s fs -put %s %s" % (HADOOP, local, remote), shell=True)

  hadoop_pipes("%s -program %s -input %s -output %s" % (
    build_d_options(wc_options), wc_script, input_, wc_output
    ))
  hadoop_pipes("%s -program %s -input %s -output %s" % (
    build_d_options(filter_options), filter_script, wc_output, filter_output
    ))


if __name__ == "__main__":
  main(sys.argv)
