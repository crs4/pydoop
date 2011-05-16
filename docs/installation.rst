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
* `Apache Hadoop <http://hadoop.apache.org>`_ version 0.20.2 or 0.21.0 or `Cloudera Hadoop <https://ccp.cloudera.com/display/SUPPORT/Downloads>`_ CDH3 Update 0
* The source code for the version of Hadoop you're using
* `Boost <http://www.boost.org>`_ version 1.40 or later (only the Python
  library).

These are also runtime requirements for all cluster nodes. Note that
installing Pydoop and your MapReduce application to all cluster nodes
(or to an NFS share) is *not* required: see :doc:`self_contained` for
a complete HowTo.


On Ubuntu
...........

On Ubuntu or Debian you can install the dependencies with the following
command::

  sudo apt-get install python libboost-python-dev


On Gentoo
...........

On Gentoo you can satisfy the dependencies with the following command::

  emerge python boost

The activated use flags per dev-libs/boost are::

  + + python        : Adds support/bindings for the Python language



Building Instructions
----------------------

Depending on how you installed Hadoop, you'll have follow the instructions
in one of the following sections.


Hadoop installed from tarball
.................................

If you have installed either Apache or Cloudera Hadoop from a tarball
follow the instructions in this section.

Set the ``HADOOP_HOME`` environment variable so that it points to where the
Hadoop tarball was extracted::

  export HADOOP_HOME=<path to Hadoop directory>

Then, in the same shell::

  tar xzf pydoop-*.tar.gz
  cd pydoop-*
  python setup.py build


Hadoop installed from Cloudera packages
.........................................


If you have installed Cloudera Hadoop on Ubuntu using the packages Cloudera
provides, then run these commands::


  sudo apt-get install libhdfs0-dev libhdfs0 hadoop-source hadoop
  tar xzf pydoop-*.tar.gz
  cd pydoop-*
  python setup.py build


Other setup
.............


If your situation isn't one of the above, you should still be able to build
Pydoop once you've installed its dependencies.

To start, extract the archive and try building::

  tar xzf pydoop-*.tar.gz
  cd pydoop-*
  python setup.py build

If the build fails, it's probably because setup.py can't find some component
critical to the building process:  the Java installation, the Hadoop
installation, or the Hadoop source code.  We can override the paths where
setup.py searches with the environment variables below.

JAVA_HOME

  By default looks  in ``/opt/sun-jdk`` and ``/usr/lib/jvm/java-6-sun``.

HADOOP_HOME

  Your Hadoop installation, containing the Hadoop jars.  By default setup.py 
  looks in ``/opt/hadoop`` and ``/usr/lib/hadoop``.


HADOOP_SRC

  Tell setup where to find the Hadoop source, if it's not under ``${HADOOP_HOME}/src`` or ``/usr/src/hadoop-*``

HADOOP_VERSION

  Override the version returned by running ``hadoop version`` (and avoid running the hadoop binary).

HADOOP_INCLUDE_PATHS

  Override the standard include paths for the Hadoop c++ headers.

Example
++++++++++

::
  
  export JAVA_HOME=/usr/local/lib/jvm
  export HADOOP_HOME=/usr/local/lib/hadoop
  export HADOOP_SRC=/var/src/hadoop-0.20.3
  tar xzf pydoop-*.tar.gz
  cd pydoop-*
  python setup.py build


Installation
----------------

In the same shell you used to run the build (in particular, with the same
environment variables still set), run one of the following installation
commands in the Pydoop distribution directory.


System-wide installation
...........................

To install in the system's ``/usr/lib`` space, run the following::

  sudo python setup.py install --skip-build


User-local installation
.........................

To install to your current user's home directory::

  python setup.py install --user

The package is installed in ``~/.local/lib/python2.6/site-packages``.
This may be a particular handy solution if your home directory is accessible on
the entire cluster.


Installing to another location
.................................

::

  python setup.py install --home <path>



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
   *at compile time* based on the output of running ``hadoop version``. If this
   fails for any reason, you can provide the correct version string
   through the ``HADOOP_VERSION`` environment variable, e.g.::

    export HADOOP_VERSION="0.21.0"


Testing Your Installation
-------------------------

After Pydoop has been successfully installed, you might want to run
unit tests to verify that everything works fine.

**IMPORTANT NOTICE:** in order to run HDFS tests you must:

#. make sure that ``HADOOP_HOME`` (and ``HADOOP_CONF_DIR``, if it does
   not coincide with ``${HADOOP_HOME}/conf``\) are set to the correct
   locations for your system

#. since one of the test cases tests the connection to an HDFS instance
   with *explicitly set* host and port, if in your case these are
   different from, respectively, "localhost" and 9000, you must set
   the ``HDFS_HOST`` and ``HDFS_PORT`` environment variables accordingly

#. start HDFS::

     ${HADOOP_HOME}/bin/start-dfs.sh

#. wait until HDFS exits from safe mode::

     ${HADOOP_HOME}/bin/hadoop dfsadmin -safemode wait

To run the unit tests, move to the ``test`` subdirectory and run *as the cluster
superuser*::

  python all_tests.py


.. note:: You can also separately run the pydoop.pipes and pydoop.hdfs tests with ``python all_tests_pipes.py`` and ``python all_tests_hdfs.py``\ .


Superuser privileges
......................


The HDFS ``chown`` tests will fail if you do not run them as a cluster 
superuser.  To have superuser privileges, you will have to either:

* start the cluster with your own user account, so you will be the cluster 
  superuser; or
* edit ``hdfs-site.xml`` in your configuration and set the ``dfs.permissions.supergroup``  
  property to one of your unix groups (type ``groups`` at the command prompt to see to 
  which groups your account belongs).



::

  <property>
    <name>dfs.permissions.supergroup</name>
    <value>mygroup</value>
  </property>


If you can't acquire superuser privileges to run the tests, just keep in mind
that the failures reported may be due to this reason.

