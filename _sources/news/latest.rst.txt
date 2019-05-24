New in 2.0.0
------------

Pydoop 2.0.0 adds Python 3 and Hadoop 3 support, and features a complete
overhaul of the ``mapreduce`` subpackage, which is now easier to use and more
efficient. As any major software release, Pydoop 2 also makes some
backwards-incompatible changes, mainly by dropping old, seldom-used
features. Finally, it includes several bug fixes and performance
improvements. Here is a more detailed list of changes:

 * Python 3 support.
 * Hadoop 3 support.
 * The ``sercore`` extension, together with most of the ``pydoop.mapreduce``
   subpackage, has been rewritten from scratch. Now it's simpler and slightly
   faster (much faster when using a combiner).
 * ``JobConf`` is now fully compatible with ``dict``.
 * ``pydoop submit`` now works when the default file system is local.
 * Compilation of avro-parquet-based examples is now much faster.
 * Many utilities for guessing Hadoop environment details have been either
   removed or drastically simplified (affects ``hadoop_utils`` and related
   package-level functions). Pydoop now assumes that the ``hadoop`` command is
   in the ``PATH``, and uses only that information to try fallback values when
   ``HADOOP_HOME`` and/or ``HADOOP_CONF_DIR`` are not defined.
 * The ``hadut`` module has been stripped down to contain little more than
   what's required by ``pydoop submit``. In particular, ``PipesRunner`` is
   gone. Running applications with ``mapred pipes`` still works, but with
   caveats (e.g., `it does not work on the local fs
   <https://issues.apache.org/jira/browse/MAPREDUCE-4000>`_, and controlling
   remote task environment is not trivial).
 * The ``hdfs`` module no longer provides a default value for ``LIBHDFS_OPTS``.
 * The Hadoop simulator has been dropped.
 * `Support for opaque binary input splits <https://github.com/crs4/pydoop/pull/302>`_.
 * `Dropped support for Hadoop 1 <https://github.com/crs4/pydoop/pull/237>`_.
 * `Dropped old MapReduce API <https://github.com/crs4/pydoop/pull/255>`_.
 * `Dropped JPype HDFS backend <https://github.com/crs4/pydoop/pull/238>`_.
 * Bug fixes and performance improvements.
