.. _installation:

Installation
============

The fastest way to try Pydoop is via the `Docker <https://www.docker.com/>`_
image::

  docker pull crs4/pydoop
  docker run -p 8020:8020 [-p ...] --name pydoop -d crs4/pydoop

Check out ``.travis.yml`` for more port bindings you probably want. This spins
up a single-node, `pseudo-distributed
<https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/SingleCluster.html#Pseudo-Distributed_Operation>`_
Hadoop cluster with `HDFS
<https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/HdfsDesign.html#Introduction>`_,
`YARN
<https://hadoop.apache.org/docs/stable/hadoop-yarn/hadoop-yarn-site/YARN.html>`_
and a Job History server. To check that all daemons are up and running, you
can run ``jps`` on the container. If everything is OK, you should get something
like this::

  $ docker exec -it pydoop bash -c 'jps | grep -v Jps'
  161 DataNode
  356 NodeManager
  523 JobHistoryServer
  75 NameNode
  301 ResourceManager

If you want to build Pydoop yourself, read on.


Supported Platforms
-------------------

At the moment, Pydoop is being tested on `CentOS <http://www.centos.org>`_ 7
only, although it should also work on other Linux distros and (possibly with
some tweaking) on macOS. Windows is **not** supported.


Prerequisites
-------------

* `Python <http://www.python.org>`_ 2 or 3 (tested with 2.7 and 3.6),
  including header files (e.g., ``python-devel`` on CentOS, ``python-dev`` on
  Debian);

* `setuptools <https://pypi.python.org/pypi/setuptools>`_ >= 3.3;

* `Apache Hadoop <http://hadoop.apache.org>`_ 2 (currently tested with 2.7,
  support for more distributions and/or versions is planned). Hadoop 1 is not
  supported anymore.

These are both build time and run time requirements. At build time only, you
will also need a C++ compiler (e.g., ``yum install gcc gcc-c++``) and a JDK
(i.e., a JRE alone is not sufficient) for Pydoop's extension modules.

**Optional:**

* `Avro <https://avro.apache.org/>`_ Python implementation to enable
  :ref:`avro_io` (run time only). Note that the pip packages for Python 2 and 3
  are named differently (respectively ``avro`` and ``avro-python3``).

* Some examples have additional requirements. Check out the Dockerfile and
  ``requirements.txt`` for details.


Environment Setup
-----------------

Pydoop needs to know where the JDK and Hadoop are installed on your
system. This is done by exporting, respectively, the ``JAVA_HOME`` and
``HADOOP_HOME`` environment variables. For instance::

  export HADOOP_HOME="/opt/hadoop-2.7.4"
  export JAVA_HOME="/usr/lib/jvm/java-8-openjdk-amd64"

If you don't know where your JDK is, find the path of the ``java`` executable::

  $ readlink -f $(which java)
  /usr/lib/jvm/java-8-openjdk-amd64/jre/bin/java

Then strip the trailing ``/jre/bin/java`` to get the ``JAVA_HOME``.


Building and Installing
-----------------------

Install prerequisites::

  pip install --upgrade pip
  pip install --upgrade -r requirements.txt

Install Pydoop via pip::

  pip install pydoop

Or get the source code and build it locally::

  git clone -b master https://github.com/crs4/pydoop.git
  cd pydoop
  python setup.py build
  python setup.py install --skip-build

In the git repository, the ``master`` branch corresponds to the latest
release, while the ``develop`` branch contains code under active development.

Note that installing Pydoop and your MapReduce applications to all cluster
nodes (or to an NFS share) is *not* required: see :doc:`self_contained` for
additional info.


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

     export HADOOP_VERSION="2.7.4"


Testing your Installation
-------------------------

After Pydoop has been successfully installed, you might want to run unit
tests and/or examples to verify that everything works fine. Here is a short
list of things that can go wrong and how to fix them. For full details on
running tests and examples, see ``.travis.yml``.

#. make sure that Pydoop is able to detect your Hadoop home and
   configuration directories.  If auto-detection fails, try setting
   the ``HADOOP_HOME`` and ``HADOOP_CONF_DIR`` environment variables
   to the appropriate locations;

#. Make sure all HDFS and YARN daemons are up (see above);

#. Wait until HDFS exits from safe mode::

     ${HADOOP_HOME}/bin/hadoop dfsadmin -safemode wait

#. HDFS tests may fail if your NameNode's hostname and port are
   non-standard. In this case, set the ``HDFS_HOST`` and ``HDFS_PORT``
   environment variables accordingly;

#. Some HDFS tests may fail if not run by the cluster superuser, in
   particular ``capacity``, ``chown`` and ``used``.  To get superuser
   privileges, you can either start the cluster with your own user account or
   set the ``dfs.permissions.superusergroup`` Hadoop property to one of your
   unix groups (type ``groups`` at the command prompt to get the list of
   groups for your current user), then restart the HDFS daemons.
