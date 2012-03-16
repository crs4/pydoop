# BEGIN_COPYRIGHT
# END_COPYRIGHT

"""
High-level API for manipulating local and HDFS files.
"""

import pydoop.hdfs as hdfs
from utils import split_hdfs_path


BUFSIZE = 16384


def dump(data, hdfs_path, **kwargs):
  """
  Write ``data`` to ``hdfs_path``.

  Additional keyword arguments, if any, are passed to :func:`open`.
  """
  kwargs["mode"] = "w"
  with hdfs.open(hdfs_path, **kwargs) as fo:
    i = 0
    while i < len(data):
      fo.write(data[i:i+BUFSIZE])
      i += BUFSIZE
  fo.fs.close()


def load(hdfs_path, **kwargs):
  """
  Read the content of ``hdfs_path`` and return it.

  Additional keyword arguments, if any, are passed to :func:`open`.
  """
  kwargs["mode"] = "r"
  data = []
  with hdfs.open(hdfs_path, **kwargs) as fi:
    while 1:
      chunk = fi.read(BUFSIZE)
      if chunk:
        data.append(chunk)
      else:
        break
  fi.fs.close()
  return "".join(data)


def cp(src_hdfs_path, dest_hdfs_path, **kwargs):
  """
  Copy the contents of ``src_hdfs_path`` to ``dest_hdfs_path``.

  Additional keyword arguments, if any, are passed to :func:`open`.
  """
  kwargs["mode"] = "r"
  with hdfs.open(src_hdfs_path, **kwargs) as fi:
    kwargs["mode"] = "w"
    with hdfs.open(dest_hdfs_path, **kwargs) as fo:
      while 1:
        chunk = fi.read(BUFSIZE)
        if chunk:
          fo.write(chunk)
        else:
          break
  for f in fi, fo:
    f.fs.close()


def lsl(hdfs_path, user=None):
  """
  Return a list of dictionaries of file properties.

  If ``hdfs_path`` is a directory, each list item corresponds to a
  file or directory contained by it; if it is a file, there is only
  one item corresponding to the file itself.
  """
  host, port, path = split_hdfs_path(hdfs_path, user)
  fs = hdfs.hdfs(host, port, user)
  dir_list = fs.list_directory(path)
  fs.close()
  return dir_list


def ls(hdfs_path, user=None):
  """
  Return a list of hdfs paths.

  If ``hdfs_path`` is a directory, each list item corresponds to a
  file or directory contained by it; if it is a file, there is only
  one item corresponding to the file itself.
  """
  dir_list = lsl(hdfs_path, user)
  return [d["name"] for d in dir_list]
