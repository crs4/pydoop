# BEGIN_COPYRIGHT
# END_COPYRIGHT

import sys, os, logging
logging.basicConfig(level=logging.INFO)

import pydoop.test_support as pts
import pydoop.hdfs as hdfs
import pydoop.hadut as hadut


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


def main(argv):
  try:
    output = argv[1]
  except IndexError:
    output = "%s/output" % HDFS_WD
  input_ = "%s/input" % HDFS_WD
  with hdfs.open(MR_SCRIPT) as f:
    pipes_code = pts.add_sys_path(f.read())
  hdfs.dump(pipes_code, MR_SCRIPT)
  logging.info("running MapReduce application")
  hadut.run_pipes(MR_SCRIPT, input_, output)


if __name__ == "__main__":
  main(sys.argv)
