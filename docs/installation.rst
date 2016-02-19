.. _installation:

Installation
============

Supported Platforms
-------------------

Linux
.....

Pydoop has been tested on `Gentoo <http://www.gentoo.org>`_, `Ubuntu
<http://www.ubuntu.com>`_ and `CentOS
<http://www.centos.org>`_. Although we currently have no information
regarding other Linux distributions, we expect Pydoop to work
(possibly with some tweaking) on them as well.

Apple OS X
..........

Pydoop has been tested on OS X 10.9 (Maverick) and OS X 10.10
(Yosemite).  Install the `Homebrew <http://brew.sh/>`_ version of
Python, then follow the instructions below.


FreeBSD
.......

We have included a patch by `trtrmitya <https://github.com/trtrmitya>`_
that adds FreeBSD support, but we have not tested it.


.. _get_pydoop:

Get Pydoop
----------

Source Distribution
...................

We recommend installing Pydoop via `pip <http://www.pip-installer.org>`_::

  pip install pydoop

To get the source code, clone our `Git <http://git-scm.com/>`_ repository::

  git clone https://github.com/crs4/pydoop.git

Where the ``master`` branch corresponds to the latest release, while
the ``develop`` branch contains code under active development.


Prerequisites
-------------

In order to build and install Pydoop, you need the following software:

* `Python <http://www.python.org>`_ version 2.7

* `setuptools <https://pypi.python.org/pypi/setuptools>`_ version 3.3
  or higher

* either of the following:

  * `Apache Hadoop <http://hadoop.apache.org>`_ version 1.0.4, 1.1.2,
    1.2.1, 2.2.0, 2.4.1, 2.5.2 or 2.6.0

  * `CDH <https://ccp.cloudera.com/display/SUPPORT/Downloads>`_
    version 4 or 5 installed from dist-specific packages or
    Cloudera Manager parcels (no tarball)

  * `HDP <http://hortonworks.com/hdp/>`_ 2.2

* `OpenSSL <http://www.openssl.org>`_

**Optional:**

* `JPype <http://jpype.sourceforge.net/>`_ to build the alternate HDFS backend

* `Avro <https://avro.apache.org/>`_ Python implementation to enable
  :ref:`avro_io`

These are also runtime requirements for all cluster nodes. Note that
installing Pydoop and your MapReduce application to all cluster nodes
(or to an NFS share) is *not* required: see :doc:`self_contained` for
additional info.
Moreover, being based on Pipes, Pydoop cannot be used with Hadoop standalone installations.

Other versions of Hadoop may or may not work depending on how
different they are from the ones listed above.


Installation
------------

Before compiling and installing Pydoop, install all missing dependencies.

In addition, if your distribution does not include them by default,
install basic development tools (such as a C/C++ compiler) and Python
header files.  On Ubuntu, for instance, you can do that as follows::

  sudo apt-get install build-essential python-dev

Set the ``JAVA_HOME`` environment variable to your JDK installation
directory, e.g.::

  export JAVA_HOME=/usr/local/java/jdk

.. note::

  If you don't know where your Java home is, try finding the actual
  path of the ``java`` executable and stripping the trailing
  ``/jre/bin/java``::

    $ readlink -f $(which java)
    /usr/lib/jvm/java-6-oracle/jre/bin/java
    $ export JAVA_HOME=/usr/lib/jvm/java-6-oracle

If you have installed Hadoop from a tarball, set the ``HADOOP_HOME``
environment variable so that it points to where the tarball was
extracted, e.g.::

  export HADOOP_HOME=/opt/hadoop-1.0.4

The above step is not necessary if you installed CDH from
dist-specific packages.  Build Pydoop with::

  python setup.py build

This builds Pydoop with the "native" HDFS backend.  To build the
(experimental) JPype backend instead, run::

  python setup.py build --hdfs-core-impl=jpype-bridged

For a system-wide installation, run the following::

  sudo python setup.py install --skip-build

For a user-local installation::

  python setup.py install --skip-build --user

The latter installs Pydoop in ``~/.local/lib/python2.X/site-packages``.
This may be a particularly handy solution if your home directory is
accessible on the entire cluster.

To install to an arbitrary path::

  python setup.py install --skip-build --home <PATH>


.. _multiple_hadoop_versions:

..
   Multiple Hadoop Versions
   ------------------------

   .. note::

     The following instructions apply to installations from
     tarballs. Running a package-based Hadoop installation together with
     a "from-tarball" one is neither advised not supported.

   If you'd like to use your Pydoop installation with multiple versions of Hadoop,
   you will need to rebuild the modules for each version of Hadoop.

   After building Pydoop for the first time following the instructions above, 
   modify your HADOOP-related environment variables to point to the other version 
   of Hadoop to be supported.  Then repeat the build and installation commands
   again.

   Example::

     export HADOOP_HOME=/opt/hadoop-1.0.4
     python setup.py install --user

     python setup.py clean --all

     export HADOOP_HOME=/opt/hadoop-1.2.1
     python setup.py install --user

   At run time, the appropriate version of the Pydoop modules will be
   loaded for the version of Hadoop selected by your ``HADOOP_HOME``
   variable.  If Pydoop is not able to retrieve your Hadoop home
   directory from the environment or by looking into standard paths, it
   falls back to a default location that is hardwired at compile time:
   the setup script looks for a file named ``DEFAULT_HADOOP_HOME`` in the
   current working directory; if the file does not exist, it is created
   and filled with the path to the current Hadoop home.


.. _troubleshooting:

Troubleshooting
---------------

#. "java home not found" error, with ``JAVA_HOME`` properly exported: try
   setting ``JAVA_HOME`` in ``hadoop-env.sh``

#. "libjvm.so not found" error: try the following::

    export LD_LIBRARY_PATH="${JAVA_HOME}/jre/lib/amd64/server:${LD_LIBRARY_PATH}"

#. non-standard include/lib directories: the setup script looks for
   includes and libraries in standard places -- read ``setup.py`` for
   details. If some of the requirements are stored in different
   locations, you need to add them to the search path. Example::

    python setup.py build_ext -L/my/lib/path -I/my/include/path -R/my/lib/path
    python setup.py build
    python setup.py install --skip-build

   Alternatively, you can write a small ``setup.cfg`` file for distutils:

   .. code-block:: cfg

    [build_ext]
    include_dirs=/my/include/path
    library_dirs=/my/lib/path
    rpath=%(library_dirs)s

   and then run ``python setup.py install``.

   Finally, you can achieve the same result by manipulating the
   environment.  This is particularly useful in the case of automatic
   download and install with pip::

    export CPATH="/my/include/path:${CPATH}"
    export LD_LIBRARY_PATH="/my/lib/path:${LD_LIBRARY_PATH}"
    pip install pydoop

#. Hadoop version issues. The Hadoop version selected at compile time is 
   automatically detected based on the output of running ``hadoop version``.
   If this fails for any reason, you can provide the correct version string
   through the ``HADOOP_VERSION`` environment variable, e.g.::

     export HADOOP_VERSION="1.0.4"


Testing your Installation
-------------------------

After Pydoop has been successfully installed, you might want to run
unit tests to verify that everything works fine.

**IMPORTANT NOTICE:** in order to run HDFS tests you must:

#. make sure that Pydoop is able to detect your Hadoop home and
   configuration directories.  If auto-detection fails, try setting
   the ``HADOOP_HOME`` and ``HADOOP_CONF_DIR`` environment variables
   to the appropriate locations;

#. since one of the test cases tests the connection to an HDFS
   instance with *explicitly set* host and port, if in your case these
   are different from, respectively, "localhost" and 9000 (8020 for
   package-based CDH), you must set the ``HDFS_HOST`` and
   ``HDFS_PORT`` environment variables accordingly;

#. start HDFS::

     ${HADOOP_HOME}/bin/start-dfs.sh

#. wait until HDFS exits from safe mode::

     ${HADOOP_HOME}/bin/hadoop dfsadmin -safemode wait

To run the unit tests, move to the ``test`` subdirectory and run *as
the cluster superuser* (see below)::

  python all_tests.py


Superuser Privileges
....................

The following HDFS tests may fail if not run by the cluster superuser:
``capacity``, ``chown`` and ``used``.  To get superuser privileges,
you can either:

* start the cluster with your own user account;

* edit ``hdfs-site.xml`` in your configuration and set the
  ``dfs.permissions.supergroup`` (``dfs.permissions.superusergroup``
  in Hadoop 2) property to one of your unix groups (type ``groups`` at
  the command prompt to see to which groups your account belongs),
  then restart the Hadoop daemons:

.. code-block:: xml

  <property>
    <name>dfs.permissions.supergroup</name>
    <value>admin</value>
  </property>

If you can't acquire superuser privileges to run the tests, just keep in mind
that the failures reported may be due to this reason.


Hadoop2 / CDH4
..............

With Apache Hadoop 2 / CDH 4, before running the unit tests, edit
``hdfs-site.xml`` and set ``dfs.namenode.fs-limits.min-block-size`` to
a low value:

.. code-block:: xml

  <property>
    <name>dfs.namenode.fs-limits.min-block-size</name>
    <value>512</value>
  </property>

then restart Hadoop daemons.
