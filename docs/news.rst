.. _news:

News
====


New in 0.12.0
-------------

 * YARN is now fully supported
 * Added support for CDH 4.4.0 and CDH 4.5.0


New in 0.11.1
-------------

 * Added support for hadoop 2.2.0
 * Added support for hadoop 1.2.1

   
New in 0.10.0
-------------

 * Added support for CDH 4.3.0

 * Added a :meth:`~pydoop.hdfs.fs.hdfs.walk` method to hdfs instances
   (works similarly to ``os.walk`` from Python's standard library)

 * The Hadoop version parser is now more flexible.  It should be able
   to parse version strings for all CDH releases, including older ones
   (note that most of them are **not** supported)

 * Pydoop script can now handle modules whose file name has no extension

 * Fixed "unable to load native-hadoop library" problem (thanks to
   Liam Slusser)


New in 0.9.0
------------

* Added explicit support for:

  * Apache Hadoop 1.1.2
  * CDH 4.2.0

* Added support for Cloudera from-parcels layout (as installed by
  Cloudera Manager)

* Added :func:`pydoop.hdfs.move`

* Record writers can now be used in map-only jobs


New in 0.8.1
------------

* Fixed a problem that was breaking installation from PyPI via pip install


New in 0.8.0
------------

* Added support for Apple OS X Mountain Lion
* Added support for Hadoop 1.1.1
* Patches now include a fix for `HDFS-829
  <https://issues.apache.org/jira/browse/HDFS-829>`_
* Restructured docs

  * A separate tutorial section collects and expands introductory material


New in 0.7.0
------------

* Added Debian package


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

* The HDFS API features new high-level tools for easier manipulation
  of files and directories. See the :ref:`API docs <hdfs-api>` for
  more info
* Examples have been thoroughly revised in order to make them easier
  to understand and run
* Several bugs were fixed; we also introduced a few optimizations,
  most notably the automatic caching of HDFS instances
* We have pushed our code to a `Git repository
  <http://sourceforge.net/p/pydoop/code>`_ hosted by `SourceForge
  <http://sourceforge.net>`_.  See the :ref:`installation` section for
  instructions.


New in 0.5.0
------------

* Pydoop now works with Hadoop 1.0
* Multiple versions of Hadoop can now be supported by the same
  installation of Pydoop.  See the section on building for
  :ref:`multiple Hadoop versions <multiple_hadoop_versions>`) for the
  details
* We have added a :ref:`command line tool <pydoop_script_tutorial>` to
  make it trivially simple to write shorts scripts for simple
  problems.
* In order to work out-of-the-box, Pydoop now requires Pydoop 2.7.
  Python 2.6 can be used provided that you install a few additional
  modules (see the :ref:`installation <installation>` page for
  details).
* We have dropped support for the 0.21 branch of Hadoop, which has
  been marked as unstable and unsupported by Hadoop developers.
