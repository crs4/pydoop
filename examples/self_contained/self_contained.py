# BEGIN_COPYRIGHT
# END_COPYRIGHT

import sys, os, subprocess as sp


HADOOP_HOME = os.environ.get("HADOOP_HOME", "/opt/hadoop")
HADOOP = os.path.join(HADOOP_HOME, "bin/hadoop")
try:
  HDFS_WD = os.environ["HDFS_WORK_DIR"]
except KeyError:
  sys.exit("ERROR: HDFS_WORK_DIR not set")
MR_SCRIPT = "%s/bin/cv" % HDFS_WD


MR_OPTIONS = {
  "mapred.job.name": "cv",
  "hadoop.pipes.java.recordreader": "true",
  "hadoop.pipes.java.recordwriter": "true",
  "mapred.cache.archives": "%s/pydoop.tgz#pydoop,%s/cv.tgz#cv" % (HDFS_WD, HDFS_WD),
  "mapred.create.symlink": "yes",
  }


def build_d_options(opt_dict):
  d_options = []
  for name, value in opt_dict.iteritems():
    d_options.append("-D %s=%s" % (name, value))
  return " ".join(d_options)


def hadoop_pipes(pipes_opts, hadoop=HADOOP):
  p = sp.Popen("%s pipes %s" % (hadoop, pipes_opts), shell=True)
  return os.waitpid(p.pid, 0)[1]


def main(argv):
  try:
    output = argv[1]
  except IndexError:
    output = "%s/output" % HDFS_WD
  input_ = "%s/input" % HDFS_WD
  d_options = build_d_options(MR_OPTIONS)
  hadoop_pipes("%s -program %s -input %s -output %s" % (
    d_options, MR_SCRIPT, input_, output
    ))


if __name__ == "__main__":
  main(sys.argv)
