# BEGIN_COPYRIGHT
# 
# Copyright 2009-2014 CRS4.
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

import sys, os, random, uuid, tempfile, imp, unittest, shutil

import pydoop


_HADOOP_HOME = pydoop.hadoop_home()
_HADOOP_CONF_DIR = pydoop.hadoop_conf()
_RANDOM_DATA_SIZE = 32
_DEFAULT_HDFS_HOST = "localhost"
_DEFAULT_HDFS_PORT = 8020 if pydoop.is_cloudera() else 9000
_DEFAULT_BYTES_PER_CHECKSUM = 512
HDFS_HOST = os.getenv("HDFS_HOST", _DEFAULT_HDFS_HOST)
HDFS_PORT = os.getenv("HDFS_PORT", _DEFAULT_HDFS_PORT)

UNI_CHR = u'\N{CYRILLIC CAPITAL LETTER O WITH DIAERESIS}'

try:
    HDFS_PORT = int(HDFS_PORT)
except ValueError:
    raise ValueError("Environment variable HDFS_PORT must be an int")

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
    if printable:
        return "".join([chr(randint(32, 126)) for _ in xrange(size)])
    else:
        return "".join([chr(randint(0, 255)) for _ in xrange(size)])


def get_bytes_per_checksum():
    params = pydoop.hadoop_params(_HADOOP_CONF_DIR, _HADOOP_HOME)
    return int(params.get('io.bytes.per.checksum',
                          params.get('dfs.bytes-per-checksum',
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


class WDTestCase(unittest.TestCase):

    def setUp(self):
        self.wd = tempfile.mkdtemp(prefix='pydoop_test_')

    def tearDown(self):
        shutil.rmtree(self.wd)

    def _mkfn(self, basename):
        return os.path.join(self.wd, basename)

    def _mkf(self, basename):
        return open(self._mkfn(basename), 'w')


  
