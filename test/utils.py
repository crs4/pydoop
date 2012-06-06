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

import os, random, uuid, tempfile, xml.dom.minidom

import pydoop
import pydoop.hadoop_utils as hu


_HADOOP_HOME = pydoop.hadoop_home()
_HADOOP_CONF_DIR = pydoop.hadoop_conf()
_RANDOM_DATA_SIZE = 32
_DEFAULT_HDFS_HOST = "localhost"
_DEFAULT_HDFS_PORT = 8020 if pydoop.is_cloudera() else 9000
_DEFAULT_BYTES_PER_CHECKSUM = 512
HDFS_HOST = os.getenv("HDFS_HOST", _DEFAULT_HDFS_HOST)
HDFS_PORT = os.getenv("HDFS_PORT", _DEFAULT_HDFS_PORT)
try:
  HDFS_PORT = int(HDFS_PORT)
except ValueError:
  raise ValueError("Environment variable HDFS_PORT must be an int")


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


def make_random_data(size=_RANDOM_DATA_SIZE):
  randint = random.randint
  return "".join([chr(randint(32, 126)) for _ in xrange(size)])


def get_bytes_per_checksum():

  def extract_text(nodes):
    return str("".join([n.data for n in nodes if n.nodeType == n.TEXT_NODE]))

  def extract_bpc(conf_path):
    dom = xml.dom.minidom.parse(conf_path)
    conf = dom.getElementsByTagName("configuration")[0]
    props = conf.getElementsByTagName("property")
    for p in props:
      n = p.getElementsByTagName("name")[0]
      if extract_text(n.childNodes) == "io.bytes.per.checksum":
        v = p.getElementsByTagName("value")[0]
        return int(extract_text(v.childNodes))
    raise IOError  # for consistency, also raised by minidom.path

  core_default = os.path.join(_HADOOP_HOME, "src", "core", "core-default.xml")
  if not os.path.exists(core_default):
    # FIXME: move source finder from setup.py to an installed module
    cloudera_src = hu.first_dir_in_glob("/usr/src/hadoop*")
    core_default = os.path.join(cloudera_src, "core", "core-default.xml")
  core_site, hadoop_site = [os.path.join(_HADOOP_CONF_DIR, fn) for fn in
                            ("core-site.xml", "hadoop-site.xml")]
  try:
    return extract_bpc(core_site)
  except IOError:
    try:
      return extract_bpc(hadoop_site)
    except IOError:
      try:
        return extract_bpc(core_default)
      except IOError:
        return _DEFAULT_BYTES_PER_CHECKSUM