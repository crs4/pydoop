# BEGIN_COPYRIGHT
# 
# Copyright 2009-2013 CRS4.
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
Counts occurrences of IP addresses in Apache access log files,
optionally ignoring IP addresses listed in a given file. The input
directory must contain files formatted as described in:

  http://httpd.apache.org/docs/1.3/logs.html#common

NOTE: the MapReduce application launched by this script is a
      Pydoop reimplementation of a Dumbo programming example from
      K. Bosteels, 'Fuzzy techniques in the usage and construction of
      comparison measures for music objects', PhD thesis, Ghent
      University, 2009, available at http://users.ugent.be/~klbostee.
"""

import sys, os, optparse, operator, logging
logging.basicConfig(level=logging.INFO)

import pydoop
import pydoop.hdfs as hdfs
import pydoop.test_support as pts
import pydoop.hadut as hadut


HADOOP_CONF_DIR = pydoop.hadoop_conf()
THIS_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_INPUT = os.path.normpath(os.path.join(THIS_DIR, "input"))
LOCAL_MR_SCRIPT = "bin/ipcount.py"
BASE_MR_OPTIONS = {
  "mapred.job.name": "ipcount",
  "mapreduce.admin.user.home.dir": os.path.expanduser("~"),
  "hadoop.pipes.java.recordreader": "true",
  "hadoop.pipes.java.recordwriter": "true",
  }
PREFIX = os.getenv("PREFIX", pts.get_wd_prefix())


def get_mr_options(opt, wd):
  mr_options = BASE_MR_OPTIONS.copy()
  if opt.exclude_fn:
    exclude_bn = os.path.basename(opt.exclude_fn)
    exclude_fn = hdfs.path.abspath(hdfs.path.join(wd, exclude_bn))
    hdfs.put(opt.exclude_fn, exclude_fn)
    mr_options["mapred.cache.files"] = "%s#%s" % (exclude_fn, exclude_bn)
    mr_options["mapred.create.symlink"] = "yes"
    mr_options["ipcount.excludes"] = exclude_bn
  return mr_options


class HelpFormatter(optparse.IndentedHelpFormatter):
  def format_description(self, description):
    return description + "\n" if description else ""


def make_parser():
  parser = optparse.OptionParser(
    usage="%prog [OPTIONS]", formatter=HelpFormatter(),
    )
  parser.set_description(__doc__.lstrip())
  parser.add_option("-i", dest="input", metavar="STRING",
                    help="input dir/file ['%default']", default=DEFAULT_INPUT)
  parser.add_option("-e", dest="exclude_fn", metavar="STRING",
                    help="exclude IPs listed in this file [None]")
  parser.add_option("-n", type="int", dest="n_top", metavar="INT",
                    help="number of top IPs to list [%default]", default=5)
  parser.add_option("-o", dest="output", metavar="STRING",
                    help="output file [stdout]", default=sys.stdout)
  return parser


def main(argv):
  parser = make_parser()
  opt, _ = parser.parse_args(argv)
  if opt.output is not sys.stdout:
    opt.output = open(opt.output, 'w')
  logger = logging.getLogger("main")
  logger.setLevel(logging.DEBUG)
  runner = hadut.PipesRunner(prefix=PREFIX, logger=logger)
  with open(LOCAL_MR_SCRIPT) as f:
    pipes_code = pts.add_sys_path(f.read())
  runner.set_input(opt.input, put=True)
  runner.set_exe(pipes_code)
  mr_options = get_mr_options(opt, runner.wd)
  runner.run(
    properties=mr_options, hadoop_conf_dir=HADOOP_CONF_DIR, logger=logger
    )
  mr_output = runner.collect_output()
  runner.clean()
  d = pts.parse_mr_output(mr_output, vtype=int)
  ip_list = sorted(d.iteritems(), key=operator.itemgetter(1), reverse=True)
  if opt.n_top:
    ip_list = ip_list[:opt.n_top]
  for ip, count in ip_list:
    opt.output.write("%s\t%d\n" % (ip, count))
  if opt.output is not sys.stdout:
    opt.output.close()


if __name__ == "__main__":
  main(sys.argv[1:])
