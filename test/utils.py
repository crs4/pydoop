# BEGIN_COPYRIGHT
# END_COPYRIGHT

import os, random

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


def make_random_data(size):
  randint = random.randint
  return "".join([chr(randint(32, 126)) for _ in xrange(size)])
