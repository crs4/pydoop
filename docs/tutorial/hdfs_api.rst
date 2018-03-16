.. _hdfs_api_tutorial:

The HDFS API
============

The :ref:`HDFS API <hdfs-api>` allows you to connect to an HDFS
installation, read and write files and get information on files,
directories and global file system properties:

.. literalinclude:: ../../examples/hdfs/repl_session.py
   :language: python
   :start-after: DOCS_INCLUDE_START
   :end-before: DOCS_INCLUDE_END


Low-level API
-------------

However convenient, the high level API showcased above is inefficient
when performing multiple operations on the same HDFS instance. This is
due to the fact that, under the hood, each function opens a separate
connection to the HDFS server and closes it before returning. The
following example shows how to build statistics of HDFS usage by block
size by directly instantiating an ``hdfs`` object, which represents an
open connection to an HDFS instance. Full source code for the example,
including a script that can be used to generate an HDFS directory tree
is located under ``examples/hdfs`` in the Pydoop distribution.

.. literalinclude:: ../../examples/hdfs/treewalk.py
   :language: python
   :start-after: DOCS_INCLUDE_START

For more information, see the :ref:`HDFS API reference <hdfs-api>`.
