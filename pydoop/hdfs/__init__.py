# BEGIN_COPYRIGHT
# END_COPYRIGHT

"""
This module allows you to connect to an HDFS installation, read and
write files and get information on files, directories and global
filesystem properties.


Configuration
-------------

The hdfs module is built on top of ``libhdfs``, in turn a JNI wrapper
around the Java fs code: therefore, for the module to work properly,
the ``CLASSPATH`` environment variable must include all paths to the
relevant Hadoop jars. Pydoop will do this for you, but it needs to
know where your Hadoop installation is located and what is your hadoop
configuration directory.

In practice, what you need to do is make sure that the ``HADOOP_HOME``
and the ``HADOOP_CONF_DIR`` (unless it coincides with
``${HADOOP_HOME}/conf``\ ) environment variables are correctly set
according to your installation. If ``HADOOP_HOME`` is not set or
empty, the hdfs module will raise an exception; if ``HADOOP_CONF_DIR``
is not set or empty, it will fall back to ``${HADOOP_HOME}/conf``\ .

Another important environment variable for this module is
``LIBHDFS_OPTS``\ . This is used to set options for the JVM on top of
which the module runs, most notably the amount of memory it uses. If
``LIBHDFS_OPTS`` is not set, the C libhdfs will let it fall back to
the default for your system, typically 1 GB. According to our
experience, this is *much* more than most applications need and adds a
lot of unnecessary memory overhead. For this reason, the hdfs module
sets ``LIBHDFS_OPTS`` to ``-Xmx48m``\ , a value that we found to be
appropriate for most applications. If your needs are different, you
can set the environment variable externally and it will override the
above setting.
"""

import os

import pydoop
import config, path as hpath
from fs import hdfs


def open(hdfs_path, mode="r", buff_size=0, replication=0, blocksize=0,
         readline_chunk_size=config.BUFSIZE, user=None):
  """
  Open a file, returning an :class:`hdfs_file` object.

  ``hdfs_path`` and ``user`` are passed to
  :func:`~path.split`, while the other args are
  passed to the :class:`hdfs_file` constructor.
  """
  host, port, path = hpath.split(hdfs_path, user)
  fs = hdfs(host, port, user)
  return fs.open_file(path, mode, buff_size, replication, blocksize,
                      readline_chunk_size)


def dump(data, hdfs_path, **kwargs):
  """
  Write ``data`` to ``hdfs_path``.

  Additional keyword arguments, if any, are passed to :func:`open`.
  """
  kwargs["mode"] = "w"
  with open(hdfs_path, **kwargs) as fo:
    i = 0
    bufsize = config.BUFSIZE
    while i < len(data):
      fo.write(data[i:i+bufsize])
      i += bufsize
  fo.fs.close()


def load(hdfs_path, **kwargs):
  """
  Read the content of ``hdfs_path`` and return it.

  Additional keyword arguments, if any, are passed to :func:`open`.
  """
  kwargs["mode"] = "r"
  data = []
  with open(hdfs_path, **kwargs) as fi:
    bufsize = config.BUFSIZE
    while 1:
      chunk = fi.read(bufsize)
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
  with open(src_hdfs_path, **kwargs) as fi:
    kwargs["mode"] = "w"
    with open(dest_hdfs_path, **kwargs) as fo:
      bufsize = config.BUFSIZE
      while 1:
        chunk = fi.read(bufsize)
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
  host, port, path = hpath.split(hdfs_path, user)
  fs = hdfs(host, port, user)
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
