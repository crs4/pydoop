# BEGIN_COPYRIGHT
#
# Copyright 2009-2019 CRS4.
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

Pydoop is a Python interface to Hadoop that allows you to write
MapReduce applications and interact with HDFS in pure Python.
"""

import os
import errno
from importlib import import_module
import pydoop.hadoop_utils as hu
from pydoop.utils.py3compat import configparser, parser_read

try:
    from pydoop.version import version as __version__
except ImportError:  # should only happen at compile time
    __version__ = None
_PATH_FINDER = hu.PathFinder()

__author__ = ", ".join((
    "Simone Leo",
    "Gianluigi Zanetti",
    "Luca Pireddu",
    "Francesco Cabras",
    "Mauro Del Rio",
    "Marco Enrico Piras",
))
__author_email__ = ", ".join((
    "<simone.leo@crs4.it>",
    "<gianluigi.zanetti@crs4.it>",
    "<luca.pireddu@crs4.it>",
    "<francesco.cabras@crs4.it>",
    "<mauro@crs4.it>",
    "<kikkomep@crs4.it>",
))
__url__ = "http://crs4.github.io/pydoop"
__propfile_basename__ = "pydoop.properties"


def reset():
    _PATH_FINDER.reset()


def hadoop_home():
    return _PATH_FINDER.hadoop_home()


def hadoop_conf():
    return _PATH_FINDER.hadoop_conf()


def hadoop_params():
    return _PATH_FINDER.hadoop_params()


def hadoop_classpath():
    return _PATH_FINDER.hadoop_classpath()


def package_dir():
    return os.path.dirname(os.path.abspath(__file__))


##############################
# Since Pydoop 1.0, we've stopped supporting installations for multiple
# Hadoop versions, so we only have a single module, so the following
# functions now return the same value regardless of the Hadoop version.
##############################

def jar_name(hadoop_vinfo=None):
    return "pydoop.jar"


def jar_path(hadoop_vinfo=None):
    path = os.path.join(package_dir(), jar_name())
    if os.path.exists(path):
        return path
    else:
        return None


def complete_mod_name(module, hadoop_vinfo=None):
    return "%s.%s" % (__package__, module)


def import_version_specific_module(name):
    return import_module(name)


# --- get properties ---
PROP_FN = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), __propfile_basename__
)


# http://stackoverflow.com/questions/2819696
class AddSectionWrapper(object):

    SEC_NAME = 'dummy'

    def __init__(self, f):
        self.f = f
        self.sechead = '[dummy]' + os.linesep

    def __iter__(self):
        return self

    def __next__(self):
        line = self.readline()
        if not line:
            raise StopIteration
        return line

    def readline(self):
        if self.sechead:
            try:
                return self.sechead
            finally:
                self.sechead = None
        else:
            return self.f.readline()


def read_properties(fname):
    parser = configparser.SafeConfigParser()
    parser.optionxform = str  # preserve key case
    try:
        with open(fname) as f:
            parser_read(parser, AddSectionWrapper(f))
    except IOError as e:
        if e.errno != errno.ENOENT:
            raise
        return None  # compile time, prop file is not there
    return dict(parser.items(AddSectionWrapper.SEC_NAME))


class LocalModeNotSupported(RuntimeError):
    def __init__(self):
        msg = 'ERROR: Hadoop is configured to run in local mode'
        super(LocalModeNotSupported, self).__init__(msg)


def check_local_mode():
    if _PATH_FINDER.is_local():
        raise LocalModeNotSupported()
