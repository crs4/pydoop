Installation-free Usage
=======================

This example shows how to use the Hadoop Distributed Cache to
distribute Python packages, possibly including Pydoop itself, to all
cluster nodes at job launch time. This is useful in all cases where
installing to each node is not feasible (e.g., lack of a shared mount
point). Of course, Hadoop itself must be already installed and
properly configured in all cluster nodes before you can run this.


Example Application: Count Vowels
---------------------------------

The example MapReduce application, "cv", is rather trivial: it counts
the occurrence of each vowel in the input text. Since the point here
is to show how a structured package can be distributed and imported,
the implementation is exceedingly verbose and inefficient.


How it Works
------------

Hadoop supports automatic distribution of files and archives to all
cluster nodes at job launch time through the Distributed Cache (DC)
feature.  The DC can be used to dispatch Python packages to all cluster
nodes, eliminating the need to install dependencies for your
application, including Pydoop itself:

.. code-block:: xml

  <property>
    <name>mapred.cache.archives</name>
    <value>pydoop.tgz#pydoop,cv.tgz#cv</value>
  </property>

  <property>
    <name>mapred.create.symlink</name>
    <value>yes</value>
  </property>

If the above snippet is added to the job configuration file, Hadoop
will look for the pydoop.tgz and cv.tgz archives in your HDFS home
(e.g., /user/simleo), copy them to all slave nodes, unpack them and
create the "pydoop" and "cv" symlinks in the current working directory
of each running task before it is executed.  If you include in each
archive the *contents* of the corresponding package, all you have to
do is add the task's cwd to the Python path:

.. code-block:: python

  import sys, os
  sys.path = [os.getcwd()] + sys.path

Of course, you have to do this *before* trying to import anything from
the distributed packages.


Running the Example
-------------------

From Pydoop's distribution root::

  cd examples/self_contained
  ./run

If something goes wrong, open the ``run`` script and add appropriate
environment configuration statements, e.g.::

  export JAVA_HOME=/my/java/home
  export HADOOP_HOME=/my/hadoop/home
  export HADOOP_CONF_DIR=/my/hadoop/conf/dir
  export LDFLAGS="-L/my/lib/path -R/my/lib/path"
  export CFLAGS=-I/my/include/path

You should run the example on a slave node. Remember that it is
supposed to work with Pydoop and cv *not* installed on the slave
nodes.

For further details, take a look at the code in ``examples/self_contained``\ .
