New in 2.0b1
------------

 * ``pydoop submit`` now works when the default file system is local.
 * Many utilities for guessing details of the Hadoop environment have been
   either removed or drastically simplified (affects ``hadoop_utils`` and
   related package-level functions). Pydoop now assumes that the ``hadoop``
   command is in the ``PATH``, and uses only that information to try fallback
   values when ``HADOOP_HOME`` and/or ``HADOOP_CONF_DIR`` are not defined.
 * The ``hadut`` module has been stripped down to contain little more than
   what's required by ``pydoop submit``. In particular, ``PipesRunner`` is
   gone. Running applications with ``mapred pipes`` still works, but with
   caveats (e.g., `it does not work on the local fs
   <https://issues.apache.org/jira/browse/MAPREDUCE-4000>`_, and controlling
   the remote task environment is not trivial).
 * The ``hdfs`` module no longer provides a default value for ``LIBHDFS_OPTS``.


New in 2.0a4
------------

 * The ``sercore`` extension, together with most of the ``pydoop.mapreduce``
   subpackage, has been rewritten from scratch. Now it's simpler and slightly
   faster (much faster when using a combiner)
 * Opaque splits are now auto-deserialized  to ``context.input_split.payload``
 * ``JobConf`` is now fully compatible with ``dict``
 * Compilation of avro-parquet-based examples is now much faster
 * The Hadoop simulator has been dropped
 * Bug fixes and performance improvements

New in 2.0a3
------------

 * Support for Hadoop 3
 * `Support for opaque binary input splits <https://github.com/crs4/pydoop/pull/302>`_
 * Moved terasort example to https://github.com/crs4/pydoop-examples

New in 2.0a2
------------

 * Support for Amazon EMR

New in 2.0a1
------------

 * Added support for Python 3
 * `Dropped support for Hadoop 1 <https://github.com/crs4/pydoop/pull/237>`_
 * `Dropped old MapReduce API <https://github.com/crs4/pydoop/pull/255>`_
 * `Dropped JPype HDFS backend <https://github.com/crs4/pydoop/pull/238>`_
 * `Added Terasort example <https://github.com/crs4/pydoop/pull/250>`_
 * Bug fixes and performance improvements
