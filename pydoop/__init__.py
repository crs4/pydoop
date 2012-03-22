# BEGIN_COPYRIGHT
# END_COPYRIGHT

# DEV NOTE: some of the variables defined here (docstring included)
# are parsed by setup.py, check it before modifying them.

"""
Pydoop: a Python MapReduce and HDFS API for Hadoop
--------------------------------------------------

Prerequisites:

* Python 2.7
* Hadoop 0.20.x, 0.20.203.x or 1.0
* Boost 1.40 or later
"""

__version__ = "0.6.0-rc1"

__author__ = "Simone Leo, Gianluigi Zanetti, Luca Pireddu"

__author_email__ = "<simone.leo@crs4.it>, <gianluigi.zanetti@crs4.it>, <luca.pireddu@crs4.it>"

__url__ = "http://pydoop.sourceforge.net"

__jar_name__ = 'pydoop.jar'

import pydoop.hadoop_utils as hu

__path_finder = hu.PathFinder()

def hadoop_home():
  global __path_finder
  return __path_finder.hadoop_home()

def hadoop_conf():
  global __path_finder
  return __path_finder.hadoop_conf()

def hadoop_version():
  global __path_finder
  return __path_finder.hadoop_version()

def is_cloudera():
  global __path_finder
  return __path_finder.cloudera()

def complete_mod_name(module, hadoop_version_tuple):
  return "%s.%s_%s" % (__package__, module, "_".join( map(str, hadoop_version_tuple)) )

def import_version_specific_module(name):
  from importlib import import_module
  low_level_mod = complete_mod_name(name, hadoop_version())
  return import_module(low_level_mod)

