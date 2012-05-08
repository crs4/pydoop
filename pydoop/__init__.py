# BEGIN_COPYRIGHT
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
except ImportError:  # should only happen at compile time
  DEFAULT_HADOOP_HOME = None
_PATH_FINDER = hu.PathFinder()

__version__ = "0.6.0-rc3"
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
