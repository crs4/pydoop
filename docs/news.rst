.. _news:

News
====


New in this Release
-------------------

High-level HDFS API
+++++++++++++++++++

The HDFS API features new high-level tools for easier manipulation of
files and directories. See the :ref:`API docs <hdfs-api>` for more
info.

Revised Examples
++++++++++++++++

Examples have been thoroughly revised in order to make them easier to
understand and run.

Bug Fixes and Optimizations
+++++++++++++++++++++++++++

Several bugs were fixed; we also introduced a few optimizations, most
notably the automatic caching of HDFS instances.


New in 0.5.0
------------

Updated for Hadoop 1.0
++++++++++++++++++++++

Pydoop now works with Hadoop 1.0.

Support for Multiple Hadoop Versions
++++++++++++++++++++++++++++++++++++++

Multiple versions of Hadoop can now be supported by the same installation of 
Pydoop.  Once you've built the module for each version of Hadoop you'd like to
use (see the :ref:`installation <installation>` page, and in particular the
section on :ref:`multiple Hadoop versions <multiple_hadoop_versions>`), the 
runtime will automatically and transparently import the right one for the 
version of Hadoop selected by you HADOOP_HOME variable.  This feature should 
make migration between Hadoop versions easier for our users.

Easy Pydoop Scripting
+++++++++++++++++++++

We have added a :ref:`command line tool <pydoop_script>` to make it
trivially simple to write shorts scripts for simple problems.  See the
:ref:`Pydoop Script <pydoop_script>` page for details.

Supported Python Versions
+++++++++++++++++++++++++

In order to work out-of-the-box, Pydoop now requires Pydoop 2.7.
Python 2.6 can be used provided that you install a few additional
modules (see the :ref:`installation <installation>` page for details).

Support for Hadoop 0.21 has been Dropped
++++++++++++++++++++++++++++++++++++++++

We have dropped support for the 0.21 branch of Hadoop, which has been
marked as unstable and unsupported by Hadoop developers.
