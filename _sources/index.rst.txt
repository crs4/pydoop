.. Pydoop documentation master file, created by
   sphinx-quickstart on Sun Jun 20 17:06:55 2010.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

**Pydoop** is a Python interface to `Hadoop
<http://hadoop.apache.org>`_ that allows you to write MapReduce
applications in pure Python:

.. literalinclude:: ../examples/wordcount/bin/wordcount_minimal.py
   :language: python
   :pyobject: Mapper

.. literalinclude:: ../examples/wordcount/bin/wordcount_minimal.py
   :language: python
   :pyobject: Reducer

Pydoop offers several features not commonly found in other Python
libraries for Hadoop:

* a rich :ref:`HDFS API <hdfs_api_tutorial>`;

* a :ref:`MapReduce API <api_tutorial>` that allows to write pure
  Python record readers / writers, partitioners and combiners;

* transparent :ref:`Avro (de)serialization <avro_io>`;

* easy :ref:`installation-free <self_contained>` usage;

Pydoop enables MapReduce programming via a pure (except for a
performance-critical serialization section) Python client for Hadoop
Pipes, and HDFS access through an extension module based on `libhdfs
<https://hadoop.apache.org/docs/r2.6.0/hadoop-project-dist/hadoop-hdfs/LibHdfs.html>`_.

To get started, read the :ref:`tutorial <tutorial>`.  Full docs,
including :ref:`installation instructions <installation>`, are listed
below.


Contents
========

.. toctree::
   :maxdepth: 2

   news/index
   tutorial/index
   installation
   pydoop_script
   running_pydoop_applications
   api_docs/index
   examples/index
   self_contained
   ideas_list
   how_to_cite


Indices and Tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
