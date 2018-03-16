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
Check that resetting the hdfs module after changing
os.environ['HADOOP_CONF_DIR'] works (i.e., Pydoop references the
correct HDFS service).

Note that it does **NOT** work if you've already instantiated an hdfs
handle, and this is NOT due to the caching system.
"""

from __future__ import print_function
import sys
import os
import argparse

import pydoop.hdfs as hdfs


def dump_status(fs):
    print("(host, port, user) = %r" % ((fs.host, fs.port, fs.user),))
    print("_CACHE = %r" % (fs._CACHE,))
    print("_ALIASES = %r" % (fs._ALIASES,))
    print()


def main(argv=sys.argv[1:]):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--conf-dir", metavar="HADOOP_CONF_DIR")
    args = parser.parse_args(argv)
    if args.conf_dir:
        os.environ["HADOOP_CONF_DIR"] = os.path.abspath(args.conf_dir)
        hdfs.reset()
    fs = hdfs.hdfs()
    print("--- OPEN ---")
    dump_status(fs)
    print("cwd:", fs.working_directory())
    print
    fs.close()
    print("--- CLOSED ---")
    dump_status(fs)


if __name__ == "__main__":
    main()
