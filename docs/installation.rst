Installation
============

Supported Platforms
-------------------

Pydoop has been tested on `Gentoo <http://www.gentoo.org>`_, `Ubuntu
<http://www.ubuntu.com>`_ and `CentOS
<http://www.centos.org>`_. Although we currently have no information
regarding other Linux distributions, we expect Pydoop to work
(possibly with some tweaking) on them as well. Platforms other than
Linux are currently not supported.


Prerequisites
-------------

In order to build and install Pydoop, you need the following software:

* `Python <http://www.python.org>`_ version 2.6
* `Hadoop <http://hadoop.apache.org>`_ version 0.20.2 or 0.21.0
* `Boost <http://www.boost.org>`_ version 1.40 or later

These are also runtime requirements for all cluster nodes. Note that
installing Pydoop and your MapReduce application to all cluster nodes
(or to an NFS share) is *not* required: see :doc:`self_contained` for
a complete HowTo.


Instructions
------------

#. set the ``JAVA_HOME`` and ``HADOOP_HOME`` environment variables to
   the correct locations for your system. setup.py defaults
   respectively to ``/opt/sun-jdk`` and ``/opt/hadoop``

#. run ``python setup.py install`` (as root) in the Pydoop
   distribution root directory

To install as an unprivileged (but sudoer) user you can run::

  export JAVA_HOME=<YOUR_JAVA_HOME>
  export HADOOP_HOME=<YOUR_HADOOP_HOME>
  python setup.py build
  sudo python setup.py install --skip-build

Finally, if you don't have root access, you can perform a local
installation (i.e., into ``~/.local/lib/python2.6/site-packages``\ )::

  export JAVA_HOME=<YOUR_JAVA_HOME>
  export HADOOP_HOME=<YOUR_HADOOP_HOME>
  python setup.py install --user

If the above does not work, please read the :ref:`troubleshooting`
section.


.. _troubleshooting:

Troubleshooting
---------------

#. non-standard include/lib directories: the setup script looks for
   includes and libraries in standard places -- read ``setup.py`` for
   details. If some of the requirements are stored in different
   locations, you need to add them to the search path. Example::

    python setup.py build_ext -L/my/lib/path -I/my/include/path -R/my/lib/path
    python setup.py build_py
    python setup.py install --skip-build

#. Hadoop version issues. The current Pydoop version supports both
   Hadoop 0.20.2 and 0.21.0. Hadoop version is automatically detected
   *at compile time* based on the contents of HADOOP_HOME. If this
   fails for any reason, you can provide the correct version string
   through the HADOOP_VERSION environment variable, e.g.::

    export HADOOP_VERSION="0.21.0"


Testing Your Installation
-------------------------

After Pydoop has been successfully installed, you might want to run
unit tests to verify that everything works fine.

**IMPORTANT NOTICE:** in order to run HDFS tests you must:

#. make sure that ``HADOOP_HOME`` (and ``HADOOP_CONF_DIR``, if it does
   not coincide with ``${HADOOP_HOME}/conf``\) are set to the correct
   locations for your system

#. since one of the test cases tests connection to an HDFS instance
   with *explicitly set* host and port, if in your case these are
   different from, respectively, "localhost" and 9000, you must set
   the ``HDFS_HOST`` and ``HDFS_PORT`` environment variables accordingly

#. start HDFS::

     ${HADOOP_HOME}/bin/start-dfs.sh

#. wait until HDFS exits from safe mode::

     ${HADOOP_HOME}/bin/hadoop dfsadmin -safemode wait

To run the unit tests, move to the ``test`` subdirectory and run::

  python all_tests.py

You can also separately run ``python all_tests_pipes.py``
and ``python all_tests_hdfs.py``\ .
