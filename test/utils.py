# BEGIN_COPYRIGHT
# END_COPYRIGHT

import os, random, uuid, tempfile, xml.dom.minidom
import pydoop

_HADOOP_HOME = pydoop.hadoop_home()
_HADOOP_CONF_DIR = pydoop.hadoop_conf()
_RANDOM_DATA_SIZE = 32
_DEFAULT_HDFS_HOST = "localhost"
_DEFAULT_HDFS_PORT = 9000
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
        return self.DEFAULT_BYTES_PER_CHECKSUM
