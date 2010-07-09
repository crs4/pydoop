HDFS: Usage by Block Size
=========================

The HDFS API allows you to connect to an HDFS installation, read and
write files and get information on files, directories and global
file system properties.

This example (stripped of imports and main for brevity) shows how the
API can be used to build statistics of HDFS usage by block size::

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


Source code for the example is located under ``examples/hdfs`` in the
Pydoop distribution.
