# BEGIN_COPYRIGHT
#
# Copyright 2009-2019 CRS4.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy
# of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.
#
# END_COPYRIGHT

"""
Utilities for unit tests.
"""

import sys
import os
import random
import uuid
import tempfile
import imp
import unittest
import shutil
import warnings
import subprocess

import pydoop
import pydoop.utils.jvm as jvm
from pydoop.utils.py3compat import StringIO

JAVA_HOME = jvm.get_java_home()
JAVA = os.path.join(JAVA_HOME, "bin", "java")
JAVAC = os.path.join(JAVA_HOME, "bin", "javac")

_RANDOM_DATA_SIZE = 32
# Default NameNode RPC port. 8020 for all versions except 3.0.0. See
# https://issues.apache.org/jira/browse/HDFS-12990
_DEFAULT_HDFS_PORT = 8020
_DEFAULT_BYTES_PER_CHECKSUM = 512


def _get_special_chr():
    """
    This is used to check unicode support.  On some systems, depending
    on locale settings, we won't be able to use non-ASCII characters
    when interacting with system calls.  Since in such cases it
    doesn't really make sense to run these tests we set UNI_CHR to a
    regular ASCII character.
    """
    # something outside the latin-1 range
    the_chr = u'\N{CYRILLIC CAPITAL LETTER O WITH DIAERESIS}'
    fd = None
    fname = None
    try:
        fd, fname = tempfile.mkstemp(suffix=the_chr)
    except UnicodeEncodeError:
        msg = ("local file system doesn't support unicode characters"
               "in filenames, falling back to ASCII-only")
        warnings.warn(msg, UnicodeWarning)
        the_chr = u's'
    finally:
        if fd:
            os.close(fd)
            os.remove(fname)
    return the_chr


UNI_CHR = _get_special_chr()

_FD_MAP = {
    "stdout": sys.stdout.fileno(),
    "stderr": sys.stderr.fileno(),
}


class FSTree(object):
    """
  >>> t = FSTree('root')
  >>> d1 = t.add('d1')
  >>> f1 = t.add('f1', 0)
  >>> d2 = d1.add('d2')
  >>> f2 = d2.add('f2', 0)
  >>> for x in t.walk(): print x.name, x.kind
  ...
  root 1
  d1 1
  d2 1
  f2 0
  f1 0
  """

    def __init__(self, name, kind=1):
        assert kind in (0, 1)  # (file, dir)
        self.name = name
        self.kind = kind
        if self.kind:
            self.children = []

    def add(self, name, kind=1):
        t = FSTree(name, kind)
        self.children.append(t)
        return t

    def walk(self):
        yield self
        if self.kind:
            for c in self.children:
                for t in c.walk():
                    yield t


def make_wd(fs, prefix="pydoop_test_"):
    if fs.host:
        wd = "%s%s" % (prefix, uuid.uuid4().hex)
        fs.create_directory(wd)
        return fs.get_path_info(wd)['name']
    else:
        return tempfile.mkdtemp(prefix=prefix)


def make_random_data(size=_RANDOM_DATA_SIZE, printable=True):
    randint = random.randint
    start, stop = (32, 126) if printable else (0, 255)
    return bytes(bytearray([randint(start, stop) for _ in range(size)]))


def get_bytes_per_checksum():
    params = pydoop.hadoop_params()
    return int(params.get('dfs.bytes-per-checksum',
                          params.get('io.bytes.per.checksum',
                                     _DEFAULT_BYTES_PER_CHECKSUM)))


def silent_call(func, *args, **kwargs):
    with open(os.devnull, "w") as dev_null:
        cache = {}
        for s in "stdout", "stderr":
            cache[s] = os.dup(_FD_MAP[s])
            os.dup2(dev_null.fileno(), _FD_MAP[s])
        try:
            ret = func(*args, **kwargs)
        finally:
            for s in "stdout", "stderr":
                os.dup2(cache[s], _FD_MAP[s])
    return ret


def get_module(name, path=None):

    fp, pathname, description = imp.find_module(name, path)
    try:
        module = imp.load_module(name, fp, pathname, description)
        return module
    finally:
        fp.close()


def compile_java(java_file, classpath, opts=None):
    if opts is None:
        opts = []
    java_class_file = os.path.splitext(
        os.path.realpath(java_file)
    )[0] + '.class'
    if (not os.path.exists(java_class_file) or
            os.path.getmtime(java_file) > os.path.getmtime(java_class_file)):
        cmd = [JAVAC] + opts
        if not {"-cp", "-classpath"}.intersection(opts):
            cmd.extend(["-cp", classpath])
        cmd.append(java_file)
        try:
            subprocess.check_call(cmd, cwd=os.path.dirname(java_file))
        except subprocess.CalledProcessError as e:
            raise RuntimeError("Error compiling Java file %s\n%s" % (
                java_file, e))


def run_java(jclass, classpath, args, wd):
    try:
        subprocess.check_call([JAVA, '-cp', classpath, jclass] + args, cwd=wd)
    except subprocess.CalledProcessError as e:
        raise RuntimeError("Error running Java class %s\n%s" % (
            jclass, e))


def get_java_output_stream(jclass, classpath, args, wd):
    output = subprocess.check_output(
        [JAVA, '-cp', classpath, jclass] + args,
        cwd=wd, stderr=open('/dev/null', 'w'))
    return StringIO(output)


class WDTestCase(unittest.TestCase):

    def setUp(self):
        self.wd = tempfile.mkdtemp(prefix='pydoop_test_')

    def tearDown(self):
        shutil.rmtree(self.wd)

    def _mkfn(self, basename):
        return os.path.join(self.wd, basename)

    def _mkf(self, basename, mode='w'):
        return open(self._mkfn(basename), mode)
