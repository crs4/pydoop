Installation
============

Supported Platforms
-------------------

Pydoop has been tested on the following Linux distros:

* `Gentoo  <http://www.gentoo.org>`_ 10.0 32/64 bit
* `Ubuntu <http://www.ubuntu.com>`_ 10.04 32/64 bit
* `CentOS <http://www.centos.org>`_ 5.2 64 bit 

Although we currently have no information regarding other Linux
distributions, we expect Pydoop to work (possibly with some tweaking)
on them as well. Platforms other than Linux are not supported.


Prerequisites
-------------

In order to build and install Pydoop, you need the following software:

* `Python <http://www.python.org>`_ version 2.6
* `Hadoop <http://hadoop.apache.org>`_ version 0.20.1 or 0.20.2
* `Boost <http://www.boost.org>`_ version 1.40 or later

These are also runtime requirements for all cluster nodes.


Instructions
------------

#. Set the ``JAVA_HOME`` and ``HADOOP_HOME`` environment variables to
   the correct locations for your system. setup.py defaults
   respectively to ``/opt/sun-jdk`` and ``/opt/hadoop``.

#. Run ``python setup.py install`` (as root) in the Pydoop
   distribution root directory.

To install as an unprivileged (but sudoer) user you can run::

  export JAVA_HOME=<YOUR_JAVA_HOME>
  export HADOOP_HOME=<YOUR_HADOOP_HOME>
  python setup.py build
  sudo python setup.py install --skip-build

Finally, if you don't have root access, you can perform a local
installation (i.e., into ``~/.local/lib/python2.X/site-packages``\ )::

  export JAVA_HOME=<YOUR_JAVA_HOME>
  export HADOOP_HOME=<YOUR_HADOOP_HOME>
  python setup.py install --user

If the above does not work, please read the :ref:`troubleshooting`
section.

**Note for Ubuntu users:** a build test with Ubuntu 9.10 64-bit and
Hadoop 0.20.1 required us to apply a patch to the original Hadoop
Pipes C++ code first. Although we recommend updating to Ubuntu 10.04
and Hadoop-0.20.2, we included the patch file (``pipes_ubuntu.patch``\ )
in Pydoop's distribution root for those who might need it.


.. _troubleshooting:

Troubleshooting
---------------

#. Missing libhdfs: Hadoop 0.20.1 does not include a pre-compiled
   version of libhdfs.so for 64-bit machines. If you are using Hadoop
   0.20.2 and/or a 32-bit system you can safely skip this. To compile
   and install your own libhdfs, do the following::

    cd ${HADOOP_HOME}
    chmod +x src/c++/{libhdfs,pipes,utils}/configure
    ant compile -Dcompile.c++=true -Dlibhdfs=true
    mv build/c++/Linux-amd64-64/lib/libhdfs.* c++/Linux-amd64-64/lib/
    cd c++/Linux-amd64-64/lib/
    ln -fs libhdfs.so.0.0.0 libhdfs.so

#. Non-standard include/lib directories: the setup script looks for
   includes and libraries in standard places -- read ``setup.py`` for
   details. If some of the requirements are stored in different
   locations, you need to add them to the search path. Example::

    python setup.py build_ext -L/my/lib/path -I/my/include/path -R/my/lib/path
    python setup.py build_py
    python setup.py install --skip-build


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
   the ``HDFS_HOST`` and ``HDFS_PORT`` environment variables accordingly.
#. start HDFS::

     ${HADOOP_HOME}/bin/start-dfs.sh

To run the unit tests, move to the ``test`` subdirectory and run::

  python all_tests.py

You can also separately run ``python all_tests_pipes.py``
and ``python all_tests_hdfs.py``\ .
