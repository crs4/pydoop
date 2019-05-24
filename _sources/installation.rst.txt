.. _installation:

Installation
============

Prerequisites
-------------

We regularly test Pydoop on Ubuntu only, but it should also work on other
Linux distros and (possibly with some tweaking) on macOS. Other platforms are
**not** supported. Additional requirements:

* `Python <http://www.python.org>`_ 2 or 3, including header files (e.g.,
  ``apt-get install python-dev``, ``yum install python-devel``);

* `setuptools <https://pypi.python.org/pypi/setuptools>`_ >= 3.3;

* Hadoop >=2. We run regular CI tests with recent versions of
  `Apache Hadoop <http://hadoop.apache.org/releases.html>`_ 2.x and 3.x,
  but we expect Pydoop to also work with other Hadoop distributions. In
  particular, we have tested it on `Amazon EMR <https://aws.amazon.com/emr>`_
  (see :ref:`emr`).

These are both build time and run time requirements. At build time you will
also need a C++ compiler (e.g., ``apt-get install build-essential``, ``yum
install gcc gcc-c++``) and a JDK (a JRE is not sufficient).

**Optional:**

* `Avro <https://avro.apache.org/>`_ Python implementation to enable
  :ref:`avro_io` (run time only). Note that the pip packages for Python 2 and 3
  are named differently (respectively ``avro`` and ``avro-python3``).


Environment Setup
-----------------

To compile the HDFS extension module, Pydoop needs the path to the JDK
installation. You can specify this via ``JAVA_HOME``. For instance::

  export JAVA_HOME="/usr/lib/jvm/java-8-openjdk-amd64"

Note that Pydoop is interested in the **JDK** home (where ``include/jni.h``
can be found), not the JRE home. Depending on your Java distribution and
version, these can be different directories (usually the former being the
latter's parent). If ``JAVA_HOME`` is not found in the environment, Pydoop
will try to locate the JDK via Java system properties.

Pydoop also includes some Java components, and it needs Hadoop libraries to be
in the ``CLASSPATH`` in order to build them. This is done by calling ``hadoop
classpath``, so make sure that the ``hadoop`` executable is in the
``PATH``. For instance, if Hadoop was installed by unpacking the tarball into
``/opt/hadoop``::

  export PATH="/opt/hadoop/bin:/opt/hadoop/sbin:${PATH}"

The Hadoop class path is also needed at run time by the HDFS extension. Again,
since Pydoop picks it up from ``hadoop classpath``, ensure that ``hadoop`` is
in the ``PATH``, as shown above. ``pydoop submit`` must also be able to call
the ``hadoop`` executable.

Additionally, Pydoop needs to read part of the Hadoop configuration to adapt
to specific scenarios. If ``HADOOP_CONF_DIR`` is in the environment, Pydoop
will try to read the configuration from the corresponding location. As a
fallback, Pydoop will also try ``${HADOOP_HOME}/etc/hadoop`` (in the above
example, ``HADOOP_HOME`` would be ``/opt/hadoop``). If ``HADOOP_HOME`` is not
defined, Pydoop will try to guess it from the ``hadoop`` executable (again,
this will have to be in the ``PATH``).


Building and Installing
-----------------------

Install prerequisites::

  pip install --upgrade pip
  pip install --upgrade -r requirements.txt

Install Pydoop via pip::

  pip install pydoop

To install a pre-release (e.g., alpha, beta) add ``--pre``::

  pip install --pre pydoop

You can also install the latest development version from GitHub::

  git clone https://github.com/crs4/pydoop.git
  cd pydoop
  python setup.py build
  python setup.py install --skip-build

If possible, you should install Pydoop on all cluster nodes. Alternatively, it
can be distributed, together with your MapReduce applications, via the Hadoop
distributed cache (see :doc:`self_contained`).


Troubleshooting
---------------

#. ``libjvm.so`` not found: try the following::

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


Testing your Installation
-------------------------

After Pydoop has been successfully installed, you might want to run unit
tests and/or examples to verify that everything works fine. Here is a short
list of things that can go wrong and how to fix them. For full details on
running tests and examples, see ``.travis.yml``.

#. Incomplete configuration: make sure that Pydoop is able to find the
   ``hadoop`` executable and configuration directory (check the above section
   on environment setup).

#. Cluster not ready: wait until all Hadoop daemons are up and HDFS exits from
   safe mode (``hadoop dfsadmin -safemode wait``).

#. HDFS tests may fail if your NameNode's hostname and port are
   non-standard. In this case, set the ``HDFS_HOST`` and ``HDFS_PORT``
   environment variables accordingly.

#. Some HDFS tests may fail if not run by the cluster superuser, in
   particular ``capacity``, ``chown`` and ``used``.  To get superuser
   privileges, you can either start the cluster with your own user account or
   set the ``dfs.permissions.superusergroup`` Hadoop property to one of your
   unix groups (type ``groups`` at the command prompt to get the list of
   groups for your current user), then restart the HDFS daemons.


.. _emr:

Using Pydoop on Amazon EMR
--------------------------

You can configure your EMR cluster to automatically install Pydoop on
all nodes via `Bootstrap Actions
<https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-plan-bootstrap.html>`_. The
main difficulty is that Pydoop relies on Hadoop being installed and
configured, even at compile time, so the bootstrap script needs to
wait until EMR has finished setting it up:

.. code-block:: bash

  #!/bin/bash
  PYDOOP_INSTALL_SCRIPT=$(cat <<EOF
  #!/bin/bash
  NM_PID=/var/run/hadoop-yarn/yarn-yarn-nodemanager.pid
  RM_PID=/var/run/hadoop-yarn/yarn-yarn-resourcemanager.pid
  while [ ! -f \${RM_PID} ] && [ ! -f \${NM_PID} ]; do
    sleep 2
  done
  export JAVA_HOME=/etc/alternatives/java_sdk
  sudo -E pip install pydoop
  EOF
  )
  echo "${PYDOOP_INSTALL_SCRIPT}" | tee -a /tmp/pydoop_install.sh
  chmod u+x /tmp/pydoop_install.sh
  /tmp/pydoop_install.sh >/tmp/pydoop_install.out 2>/tmp/pydoop_install.err &

The bootstrap script creates the actual installation script and calls
it; the latter, in turn, waits for either the resource manager or the
node manager to be up (i.e., for YARN to be up whether we are on
the master or on a slave) before installing Pydoop. If you want to use
Python 3, install version 3.6 with yum:

.. code-block:: bash

  #!/bin/bash
  sudo yum -y install python36-devel python36-pip
  sudo alternatives --set python /usr/bin/python3.6
  PYDOOP_INSTALL_SCRIPT=$(cat <<EOF
  ...

The above instructions have been tested on ``emr-5.12.0``.


Trying Pydoop without installing it
-----------------------------------

You can try Pydoop on a `Docker <https://www.docker.com/>`_ container. The
Dockerfile is in the distribution root directory::

  docker build -t pydoop .
  docker run --name pydoop -d pydoop

This spins up a single-node, `pseudo-distributed
<https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/SingleCluster.html#Pseudo-Distributed_Operation>`_
Hadoop cluster with `HDFS
<https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/HdfsDesign.html#Introduction>`_,
`YARN
<https://hadoop.apache.org/docs/stable/hadoop-yarn/hadoop-yarn-site/YARN.html>`_
and a Job History server. Before attempting to use the container, wait a few
seconds until all daemons are up and running.

You may want to expose some ports to the host, such as the ones used by the
web interfaces. For instance::

  docker run --name pydoop -p 8088:8088 -p 9870:9870 -p 19888:19888 -d pydoop

Refer to the Hadoop docs for a complete list of ports used by the various
services.
