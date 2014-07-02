.. _installation:

Installation
============

Supported Platforms
-------------------

Pydoop has been tested on `Gentoo <http://www.gentoo.org>`_, `Ubuntu
<http://www.ubuntu.com>`_ and `CentOS
<http://www.centos.org>`_. Although we currently have no information
regarding other Linux distributions, we expect Pydoop to work
(possibly with some tweaking) on them as well.

We also have a :ref:`walkthrough <osx>` for compiling and installing
on `Apple OS X Mountain Lion <http://www.apple.com/osx>`_.

Other platforms are not supported.

.. _get_pydoop:

Get Pydoop
----------

Source Distribution
...................

We recommend downloading the latest release from
https://sourceforge.net/projects/pydoop/files.

You can also get the latest code from the `Git <http://git-scm.com/>`_
repository::

  git clone https://github.com/crs4/pydoop.git

We also upload our releases to `PyPI <http://pypi.python.org>`_.
After configuring your environment (see below), you should be able to
automatically download and install Pydoop from PyPI using `pip
<http://www.pip-installer.org>`_::

  pip install pydoop


Debian/Ubuntu Package
.....................

Download the latest .deb package from
https://sourceforge.net/projects/pydoop/files.


Prerequisites
-------------

In order to build and install Pydoop, you need the following software:

* `Python <http://www.python.org>`_ version 2.7 (or 2.6 with
  backports [#]_)

* either of the following:

  * `Apache Hadoop <http://hadoop.apache.org>`_ version 0.20.2, 1.0.4,
    1.1.2, 1.2.1 or 2.2.0
  * `CDH <https://ccp.cloudera.com/display/SUPPORT/Downloads>`_
    version 3u4, 3u5, 4.2.0 or 4.3.0, with the following limitations:

    * currently, only mrv1 is supported
    * CDH4 must be installed from dist-specific packages (no tarball)

* `Boost <http://www.boost.org>`_ version 1.40 or later (only the Python
  library)

* `OpenSSL <http://www.openssl.org>`_ (not required with Hadoop 0.20.2)

These are also runtime requirements for all cluster nodes. Note that
installing Pydoop and your MapReduce application to all cluster nodes
(or to an NFS share) is *not* required: see :doc:`self_contained` for
a complete HowTo.

Other versions of Hadoop may or may not work depending on how
different they are from the ones listed above.


Installation
------------

Ubuntu
......

On Ubuntu you should install the .deb package (see the :ref:`Get
Pydoop <get_pydoop>` section) corresponding to the CDH version you are
running (if you are using Apache Hadoop, try :ref:`building Pydoop
from source<from_source>` instead).  Our .deb packages have been
tested on 64-bit Ubuntu 12.04 LTS (Precise Pangolin) with the
following prerequisites installed:

* Python 2.7, with python-support
* Boost.Python 1.46.1
* CDH
* Oracle JDK 6

  * Follow `these instructions
    <http://superuser.com/questions/353983/how-do-i-install-the-sun-java-sdk-in-ubuntu-11-10-oneric-and-later-versions>`_.
    Another option is to create a local repository with `oab
    <https://github.com/flexiondotorg/oab-java6>`_.

If the above prerequisites are satisfied, you should be able to
install Pydoop by doing::

  sudo dpkg -i <PATH_TO_PYDOOP_DEB_PKG>

The following is a complete walkthrough for CDH4 that merges all of
the above instructions (tested on an empty box):

.. code-block:: bash

  # install canonical dependencies
  sudo apt-get install libboost-python1.46.1 python-support
  # remove openjdk if necessary
  sudo apt-get purge openjdk*
  # add repositories for CDH4 and Oracle Java
  sudo sh -c "echo 'deb [arch=amd64] http://archive.cloudera.com/cdh4/ubuntu/precise/amd64/cdh precise-cdh4 contrib' > /etc/apt/sources.list.d/cloudera.list"
  sudo sh -c "echo 'deb-src http://archive.cloudera.com/cdh4/ubuntu/precise/amd64/cdh precise-cdh4 contrib' >> /etc/apt/sources.list.d/cloudera.list"
  sudo apt-get install curl
  curl -s http://archive.cloudera.com/cdh4/ubuntu/precise/amd64/cdh/archive.key | sudo apt-key add -
  sudo apt-get install python-software-properties
  sudo add-apt-repository ppa:eugenesan/java
  sudo apt-get update
  # install Oracle Java and CDH4 with mrv1
  sudo apt-get install oracle-java6-installer
  cd /usr/lib/jvm && sudo ln -s java-6-oracle java-6-sun
  sudo apt-get install hadoop-0.20-conf-pseudo hadoop-client
  # install Pydoop
  sudo dpkg -i <PATH_TO_PYDOOP_DEB_PKG>


.. _from_source:

Installation from Source
........................

Before compiling and installing Pydoop, install all missing dependencies.

On Ubuntu::

  sudo apt-get install build-essential python-all-dev libboost-python-dev libssl-dev

On Gentoo::

  echo 'dev-libs/boost python' >> /etc/portage/package.use
  emerge boost openssl

If you're using Boost version 1.48 or newer, you may need to specify the
name of your Boost.Python library in order to build Pydoop. This is
done via the ``BOOST_PYTHON`` environment variable. For instance::

  export BOOST_PYTHON=boost_python-2.7

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
dist-specific packages.  Build Pydoop with the following commands::

  tar xzf pydoop-*.tar.gz
  cd pydoop-*
  python setup.py build

For a system-wide installation, run the following::

  sudo python setup.py install --skip-build

For a user-local installation::

  python setup.py install --skip-build --user

The latter installs Pydoop in ``~/.local/lib/python2.X/site-packages``.
This may be a particularly handy solution if your home directory is
accessible on the entire cluster.

To install to an arbitrary path::

  python setup.py install --skip-build --home <PATH>


.. _osx:

Installation on Apple OS X Mountain Lion
----------------------------------------

To build Pydoop on OS X you need the following prerequisites:

* `Oracle JDK
  <http://www.oracle.com/technetwork/java/javase/overview/index.html>`_
  (follow Downloads -> JDK and select the .dmg package for OS X);
* Command line tools for Xcode from the `Apple Developer Tools
  <https://developer.apple.com/downloads>`_;
* `Homebrew <http://mxcl.github.com/homebrew>`_.

Install Boost::

  brew install boost --build-from-source

See `the common issues section of the Homebrew docs
<https://github.com/mxcl/homebrew/wiki/Common-Issues>`_ for more info
on why we need the ``--build-from-source`` switch.

Install Hadoop::

  brew install hadoop

You may follow `this guide
<http://ragrawal.wordpress.com/2012/04/28/installing-hadoop-on-mac-osx-lion>`_
for Hadoop installation and configuration.

Set ``JAVA_HOME`` according to your JDK installation, e.g.::

  export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk1.7.0_17.jdk/Contents/Home

To install Pydoop via Homebrew::

  brew tap samueljohn/python
  brew install pydoop

To compile and install from source, follow the instructions in the
previous section, configuring the environment as follows::

  export HADOOP_HOME=/usr/local/Cellar/hadoop/1.1.2/libexec
  export BOOST_PYTHON=boost_python-mt


.. _multiple_hadoop_versions:

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

  tar xzf pydoop-*.tar.gz
  cd pydoop-*

  export HADOOP_HOME=/opt/hadoop-0.20.2
  python setup.py install --user

  python setup.py clean --all

  export HADOOP_HOME=/opt/hadoop-1.0.4
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
  ``dfs.permissions.supergroup`` property to one of your unix groups
  (type ``groups`` at the command prompt to see to which groups your
  account belongs), then restart the Hadoop daemons:

.. code-block:: xml

  <property>
    <name>dfs.permissions.supergroup</name>
    <value>admin</value>
  </property>

If you can't acquire superuser privileges to run the tests, just keep in mind
that the failures reported may be due to this reason.


Hadoop 2.2.0
....................

In Hadoop 2.2.0 it is necessary to edit ``hdfs-site.xml`` and set dfs.namenode.fs-limits.min-block-size to a low value:

.. code-block:: xml

  <property>
    <name>dfs.namenode.fs-limits.min-block-size</name>
    <value>512</value>
  </property>


then restart Hadoop daemons.


Using Pydoop with YARN
....................

Since Hadoop 2.* and CDH 4.* it is possible to run YARN, the next generation MapReduce framework. Using Pydoop with YARN does not require any further configuration -- of course, you need a properly configured Hadoop cluster, see:
 - http://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/ClusterSetup.html
 - http://www.cloudera.com/content/cloudera-content/cloudera-docs/CDH4/4.3.0/CDH4-Installation-Guide/cdh4ig_topic_11_4.html



.. rubric:: Footnotes

.. [#] To make Pydoop work with Python 2.6 you need to install the
   following additional modules: `importlib
   <http://pypi.python.org/pypi/importlib>`_ and `argparse
   <http://pypi.python.org/pypi/argparse>`_.
