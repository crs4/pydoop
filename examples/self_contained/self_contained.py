# BEGIN_COPYRIGHT
# END_COPYRIGHT

import sys, os, logging, re
logging.basicConfig(level=logging.INFO)

import pydoop.test_support as pts
import pydoop.hdfs as hdfs
import pydoop.hadut as hadut


try:
  from collections import Counter  # new in Python 2.7
except ImportError:
  class Counter(dict):
    def __init__(self, seq):
      super(Counter, self).__init__()
      for x in seq:
        self[x] = self.get(x, 0) + 1


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
  "mapred.cache.archives": "{0}/pydoop.tgz#pydoop,{0}/cv.tgz#cv".format(
    HDFS_WD
    ),
  "mapred.create.symlink": "yes",
  }


def local_vc(input_dir):
  input_data = []
  for fn in os.listdir(input_dir):
    if fn[0] == ".":
      continue
    with open(os.path.join(input_dir, fn)) as f:
      input_data.append(f.read())
  input_data = "".join(input_data)
  vowels = re.findall('[AEIOUY]', input_data.upper())
  return Counter(vowels)


def check(res, expected_res):
  res = pts.compare_counts(pts.parse_mr_output(res, vtype=int), expected_res)
  if res:
    return "ERROR: %s" % res
  else:
    return "OK."


def main(argv):
  local_input = argv[1]
  output = argv[2]
  input_ = "%s/input" % HDFS_WD
  with hdfs.open(MR_SCRIPT) as f:
    pipes_code = pts.add_sys_path(f.read())
  hdfs.dump(pipes_code, MR_SCRIPT)
  logging.info("running MapReduce application")
  hadut.run_pipes(MR_SCRIPT, input_, output)
  res = pts.collect_output(output)
  expected_res = local_vc(local_input)
  logging.info(check(res, expected_res))


if __name__ == "__main__":
  main(sys.argv)
