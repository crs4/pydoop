.. _installation:

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


Get Pydoop
----------

We recommend downloading the latest release from
https://sourceforge.net/projects/pydoop/files.

You can also get the latest code from the `Git <http://git-scm.com/>`_
repository::

  git clone git://pydoop.git.sourceforge.net/gitroot/pydoop/pydoop pydoop

We also upload our releases to `PyPI <http://pypi.python.org>`_.
After configuring your environment (see below), you should be able to
automatically download and install Pydoop from PyPI using `pip
<http://www.pip-installer.org>`_::

  pip install pydoop


Prerequisites
-------------

In order to build and install Pydoop, you need the following software:

* `Python <http://www.python.org>`_ version 2.7 (or 2.6 with
  backports [#]_)

* either of the following:

  * `Apache Hadoop <http://hadoop.apache.org>`_ version 0.20.2
  * `Apache Hadoop <http://hadoop.apache.org>`_ version 1.0 (tested with 1.0.3)
  * `CDH <https://ccp.cloudera.com/display/SUPPORT/Downloads>`_ version 3
    (tested with update 4)

* `Boost <http://www.boost.org>`_ version 1.40 or later (only the Python
  library)

* `OpenSSL <http://www.openssl.org>`_ (not required with Hadoop 0.20)

These are also runtime requirements for all cluster nodes. Note that
installing Pydoop and your MapReduce application to all cluster nodes
(or to an NFS share) is *not* required: see :doc:`self_contained` for
a complete HowTo.


On Ubuntu
.........

On Debian/Ubuntu you can install the dependencies with the following
command::

  sudo apt-get install python libboost-python-dev openssl


On Gentoo
.........

We haven't released an ebuild for Pydoop yet. Emerge dependencies as follows::

  echo 'dev-libs/boost python' >> /etc/portage/package.use
  emerge boost openssl

If you're using Boost version 1.48 or newer, you need to specify the
name of your Boost.Python library in order to build Pydoop. This is
done via the ``BOOST_PYTHON`` environment variable. For instance::

  export BOOST_PYTHON=boost_python-2.7


Building Instructions
----------------------

Depending on how you installed Hadoop, you'll have to follow the instructions
in one of the following sections.


Hadoop Installed from Tarball
.............................

If you have installed either Apache or Cloudera Hadoop from a tarball,
follow the instructions in this section.

Set the ``HADOOP_HOME`` environment variable so that it points to where the
Hadoop tarball was extracted::

  export HADOOP_HOME=<path to Hadoop directory>

Then, in the same shell::

  tar xzf pydoop-*.tar.gz
  cd pydoop-*
  python setup.py build


Hadoop Installed from Cloudera Debian Packages
..............................................

If you have installed Hadoop on Debian/Ubuntu using the packages
Cloudera provides, you have to make sure that the source code is
installed as well::

  sudo apt-get install libhdfs0-dev hadoop-source

Then you can unpack and build Pydoop as shown above.


Other Setup
...........

If the build fails, it's probably because setup.py can't find some component
critical to the building process:  the Java installation, the Hadoop
installation, or the Hadoop source code.  We can override the paths where
setup.py searches with the environment variables below:

* JAVA_HOME, e.g., ``/opt/sun-jdk``
* HADOOP_HOME, e.g., ``/opt/hadoop-1.0.3``
* HADOOP_CPP_SRC, e.g., ``/usr/src/hadoop-0.20/c++``
* MAPRED_INCLUDE, HDFS_INCLUDE, HDFS_LINK: colon-separated directories
  contaning, respectively, MapReduce header files, HDFS header files
  and the HDFS C library.


Installation
------------

In the same shell you used to run the build (in particular, with the same
environment variables still set), run one of the following installation
commands in the Pydoop distribution directory.


System-wide Installation
........................

To install in the system's ``/usr/lib`` space, run the following::

  sudo python setup.py install --skip-build


User-local Installation
.......................

To install to your current user's home directory::

  python setup.py install --user

The package is installed in ``~/.local/lib/python2.7/site-packages``.
This may be a particularly handy solution if your home directory is
accessible on the entire cluster.


Installing to Another Location
..............................

::

  python setup.py install --home <path>


.. _multiple_hadoop_versions:

Multiple Hadoop Versions
------------------------

.. note::

  The following instructions apply to installations from
  tarballs. Running a package-based Hadoop installation together with
  a "from-tarball" one is **not** supported.

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

  export HADOOP_HOME=/opt/hadoop-1.0.3
  python setup.py install --user

At run time, the appropriate version of the Pydoop modules will be
loaded for the version of Hadoop selected by your ``HADOOP_HOME``
variable.  If Pydoop is not able to retrieve your Hadoop home
directory from the environment or by looking into standard paths, it
falls back to a default location that is hardwired at compile time:
the setup script looks for a file named ``DEFAULT_HADOOP_HOME`` in the
current working directory; if the file does not exist, it is created
and filled with the path to the current Hadoop home (at compile time,
Pydoop must *always* know where the Hadoop home is).


.. _troubleshooting:

Troubleshooting
---------------

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

   In the case of automatic download and install with pip, try the
   following instead::

    export CPATH="/my/include/path:${CPATH}"
    export LD_LIBRARY_PATH="/my/lib/path:${LD_LIBRARY_PATH}"
    pip install pydoop

#. Hadoop version issues. The Hadoop version selected at compile time is 
   automatically detected based on the output of running ``hadoop version``.
   If this fails for any reason, you can provide the correct version string
   through the ``HADOOP_VERSION`` environment variable, e.g.::

     export HADOOP_VERSION="1.0.3"


Testing your Installation
-------------------------

After Pydoop has been successfully installed, you might want to run
unit tests to verify that everything works fine.

**IMPORTANT NOTICE:** in order to run HDFS tests you must:

#. make sure that Pydoop is able to detect your Hadoop home and
   configuration directories. If auto-detection fails, try setting the
   ``HADOOP_HOME`` and ``HADOOP_CONF_DIR`` environment variables to
   the appropriate locations;

#. since one of the test cases tests the connection to an HDFS
   instance with *explicitly set* host and port, if in your case these
   are different from, respectively, "localhost" and 9000 (8020 for
   CDH-based installations), you must set the ``HDFS_HOST`` and
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


.. rubric:: Footnotes

.. [#] To make Pydoop work with Python 2.6 you need to install the
   following additional modules: `importlib
   <http://pypi.python.org/pypi/importlib>`_ and `argparse
   <http://pypi.python.org/pypi/argparse>`_.
