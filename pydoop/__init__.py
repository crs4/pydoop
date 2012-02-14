# BEGIN_COPYRIGHT
# END_COPYRIGHT

# DEV NOTE: some of the variables defined here (docstring included)
# are parsed by setup.py, check it before modifying them.

"""
Pydoop: a Python MapReduce and HDFS API for Hadoop
--------------------------------------------------

Prerequisites:

* Python 2.6
* Hadoop 0.20.x or 1.0
* Boost 1.40 or later
"""

__version__ = "0.4.0_rc3"

__author__ = "Simone Leo, Gianluigi Zanetti, Luca Pireddu"

__author_email__ = "<simone.leo@crs4.it>, <gianluigi.zanetti@crs4.it>, <luca.pireddu@crs4.it>"

__url__ = "http://pydoop.sourceforge.net"

import os
import pydoop.hadoop_utils as hu

HadoopHome = None
HadoopConfDir = None
HadoopVersion = None

def hadoop_home():
  global HadoopHome
  if HadoopHome is None:
    try:
      HadoopHome = os.environ["HADOOP_HOME"]
    except KeyError:
      raise ValueError("HADOOP_HOME not set")
  return HadoopHome

def hadoop_conf():
  global HadoopConfDir
  if HadoopConfDir is None:
    try:
      HadoopConfDir = os.environ["HADOOP_CONF_DIR"]
    except KeyError:
      conf_dir = os.path.join(hadoop_home(), 'conf')
      if os.path.isdir(conf_dir):
        HadoopConfDir = conf_dir
      else:
        raise ValueError("Cannot determine HADOOP_CONF_DIR")
  return HadoopConfDir


def hadoop_version():
  global HadoopVersion
  if HadoopVersion is None:
    HadoopVersion = hu.get_hadoop_version( hadoop_home() )
  return HadoopVersion

def complete_mod_name(module, hadoop_version_tuple):
  return "%s.%s_%s" % (__package__, module, "_".join( map(str, hadoop_version_tuple)) )

def import_version_specific_module(name):
  from importlib import import_module
  low_level_mod = complete_mod_name(name, hadoop_version())
  return import_module(low_level_mod)
