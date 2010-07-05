A Self-contained Example
========================

This example shows how to use the Hadoop Distributed Cache to
distribute Python packages, possibly including Pydoop itself, to all
cluster nodes at job launch time. This is useful in all cases where
installing to each node is not feasible (e.g., lack of a shared mount
point). Of course, Hadoop itself must be already installed and
properly configured in all cluster nodes before you can run this.


Example Application: Count Vowels
---------------------------------

The example MapReduce application, "cv", is rather trivial: it counts
the occurrence of each vowel in the input text. Since the point here is
to show how a structured package can be distributed and imported, the
implementation is exceedingly verbose and inefficient. The included
input text is a free version of Lewis Carrol's "Alice's Adventures in
Wonderland" from Project Gutenberg (see the "input" sub-directory), but
you can use any combination of text files if you wish.


How it works
------------

Hadoop supports automatic distribution of files and archives to all
cluster nodes at job launch time through the Distributed Cache (DC)
feature. The DC can be used to dispatch Python packages to all cluster
nodes, eliminating the need to install dependencies for your
application, including Pydoop itself::

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
(e.g., /user/simone), copy them to all slave nodes, unpack them and
create the "pydoop" and "cv" symlinks in the current working directory
of each running task before it is executed. If you include in each
archive the *contents* of the corresponding package, all you have to
do is add the task's cwd to the Python path::

  import sys, os
  sys.path = [os.getcwd()] + sys.path

Of course, you have to do this *before* trying to import anything from
the distributed packages.

The Makefile includes everything you need to build and run the example
from scratch. Before doing this, review it and see if there is any
variable that does not fit with your environment. For example, on
Gentoo Linux you might want to run something like this::

  make JAVA_HOME=/etc/java-config-2/current-system-vm run

You should run it on a slave node or on an identical machine. Remember
that it is supposed to work with Pydoop and cv *not* installed on the
slave nodes.

For further details, take a look at the source code and other files
included in this example.
