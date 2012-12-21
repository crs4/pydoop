.. _hdfs_api_tutorial:

The HDFS API
============

The :ref:`HDFS API <hdfs-api>` allows you to connect to an HDFS
installation, read and write files and get information on files,
directories and global file system properties:

.. code-block:: python

  >>> import pydoop.hdfs as hdfs
  >>> hdfs.mkdir("test")
  >>> hdfs.dump("hello", "test/hello.txt")
  >>> text = hdfs.load("test/hello.txt")
  >>> print text
  hello
  >>> hdfs.ls("test")
  ['hdfs://localhost:9000/user/simleo/test/hello.txt']
  >>> for k, v in hdfs.lsl("test")[0].iteritems():
  ...     print "%s = %s" % (k, v)
  ...
  kind = file
  group = supergroup
  name = hdfs://localhost:9000/user/simleo/test/hello.txt
  last_mod = 1333119543
  replication = 1
  owner = simleo
  permissions = 420
  block_size = 67108864
  last_access = 1333119543
  size = 5
  >>> hdfs.cp("test", "test.copy")
  >>> hdfs.ls("test.copy")
  ['hdfs://localhost:9000/user/simleo/test.copy/hello.txt']
  >>> hdfs.get("test/hello.txt", "/tmp/hello.txt")
  >>> with open("/tmp/hello.txt") as f:
  ...     print f.read()
  ...
  hello
  >>> hdfs.put("/tmp/hello.txt", "test.copy/hello.txt.copy")
  >>> for x in hdfs.ls("test.copy"): print x
  ...
  hdfs://localhost:9000/user/simleo/test.copy/hello.txt
  hdfs://localhost:9000/user/simleo/test.copy/hello.txt.copy
  >>> with hdfs.open("test/hello.txt", "r") as fi:
  ...     print fi.read(3)
  ...
  hel


Low-level API
-------------

Pydoop's HDFS API can also be used at a lower level, which mirrors
Hadoop's C HDFS API (libhdfs). The following example
shows how to build statistics of HDFS usage by block size by directly
instantiating an ``hdfs`` object, which represents an open connection
to an HDFS instance:

.. code-block:: python

  import pydoop.hdfs as hdfs

  ROOT = "pydoop_test_tree"

  def treewalker(fs, root_info):
    yield root_info
    if root_info['kind'] == 'directory':
      for info in fs.list_directory(root_info['name']):
        for item in treewalker(fs, info):
          yield item

  def usage_by_bs(fs, root):
    stats = {}
    root_info = fs.get_path_info(root)
    for info in treewalker(fs, root_info):
      if info['kind'] == 'directory':
        continue
      bs = int(info['block_size'])
      size = int(info['size'])
      stats[bs] = stats.get(bs, 0) + size
    return stats

  if __name__ == "__main__":
    fs = hdfs.hdfs()
    print "BS (MB)\tBYTES USED"
    for k, v in sorted(usage_by_bs(fs, ROOT).iteritems()):
      print "%d\t%d" % (k/2**20, v)
    fs.close()

Full source code for the example is located under ``examples/hdfs`` in
the Pydoop distribution.  You should be able to run the example by
doing, from the Pydoop root directory::

  cd examples/hdfs
  ./run [TREE_DEPTH] [TREE_SPAN]

See the :ref:`HDFS API reference <hdfs-api>` for a complete feature list.
