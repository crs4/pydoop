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

from importlib import import_module
import pydoop.hadoop_utils as hu

_PATH_FINDER = hu.PathFinder()

__version__ = "0.6.0-rc2"
__author__ = "Simone Leo, Gianluigi Zanetti, Luca Pireddu"
__author_email__ = "<simone.leo@crs4.it>, <gianluigi.zanetti@crs4.it>, <luca.pireddu@crs4.it>"
__url__ = "http://pydoop.sourceforge.net"
__jar_name__ = 'pydoop.jar'


def hadoop_home():
  return _PATH_FINDER.hadoop_home()


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


def import_version_specific_module(name):
  low_level_mod = complete_mod_name(name, hadoop_version())
  return import_module(low_level_mod)
