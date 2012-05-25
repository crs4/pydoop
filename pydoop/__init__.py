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

# DEV NOTE: some of the variables defined here (docstring included)
# are parsed by setup.py, check it before modifying them.

"""
Pydoop: a Python MapReduce and HDFS API for Hadoop
--------------------------------------------------

Pydoop provides a MapReduce and HDFS API for Hadoop that allows
object-oriented Java-style MapReduce programming in CPython as well as
HDFS access.
"""

import os
from importlib import import_module
import pydoop.hadoop_utils as hu

try:
  from config import DEFAULT_HADOOP_HOME
  from version import version as __version__
except ImportError:  # should only happen at compile time
  DEFAULT_HADOOP_HOME = __version__ = None
_PATH_FINDER = hu.PathFinder()

__author__ = "Simone Leo, Gianluigi Zanetti, Luca Pireddu"
__author_email__ = "<simone.leo@crs4.it>, <gianluigi.zanetti@crs4.it>, <luca.pireddu@crs4.it>"
__url__ = "http://pydoop.sourceforge.net"
__jar_name__ = 'pydoop.jar'


def hadoop_home(fallback=DEFAULT_HADOOP_HOME):
  return _PATH_FINDER.hadoop_home(fallback=fallback)


def hadoop_conf():
  return _PATH_FINDER.hadoop_conf()


def hadoop_version():
  return _PATH_FINDER.hadoop_version()


def is_cloudera():
  return _PATH_FINDER.cloudera()


def complete_mod_name(module, hadoop_version_tuple):
  return "%s.%s_%s" % (
    __package__, module, "_".join(map(str, hadoop_version_tuple))
    )

def jar_path():
  possible_path = os.path.join(os.path.dirname(__file__), __jar_name__)
  if os.path.exists(possible_path):
    return possible_path
  else:
    return None


def import_version_specific_module(name):
  low_level_mod = complete_mod_name(name, hadoop_version())
  return import_module(low_level_mod)
