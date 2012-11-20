.. _news:

News
====


New in 0.7.0-rc3
----------------

* Fixed a bug in the hdfs instance caching method

New in 0.7.0-rc2
----------------

* Support for HDFS append open mode

  * fails if your Hadoop version and/or configuration does not support
    HDFS append

New in 0.7.0-rc1
----------------

* Works with CDH4, with the following limitations:

  * support for MapReduce v1 only
  * CDH4 must be installed from dist-specific packages (no tarball)

* Tested with the latest releases of other Hadoop versions

  * Apache Hadoop 0.20.2, 1.0.4
  * CDH 3u5, 4.1.2

* Simpler build process

  * the source code we need is now included, rather than searched for
    at compile time

* Pydoop scripts can now accept user-defined configuration parameters

  * New examples show how to use the new feature

* New wrapper object makes it easier to interact with the JobConf
* New hdfs.path functions: isdir, isfile, kind
* HDFS: support for string description of permission modes in chmod
* Several bug fixes


New in 0.6.6
------------

Fixed a bug that was causing the pipes runner to incorrectly preprocess
command line options.


New in 0.6.4
------------

Fixed several bugs triggered by using a local fs as the default fs for
Hadoop.  This happens when you set a ``file:`` path as the value of
``fs.default.name`` in core-site.xml.  For instance:

.. code-block:: xml

  <property>
    <name>fs.default.name</name>
    <value>file:///var/hadoop/data</value>
  </property>


New in 0.6.0
------------

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

Git Repository
++++++++++++++

We have pushed our code to a `Git repository
<http://pydoop.git.sourceforge.net/git/gitweb.cgi?p=pydoop/pydoop>`_
hosted by Sourceforge.  See the :ref:`installation` section for
instructions.


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
