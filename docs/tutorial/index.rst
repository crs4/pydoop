.. _tutorial:

Tutorial
========

Pydoop is a package that provides a Python API for `Hadoop
<http://hadoop.apache.org>`_ MapReduce and HDFS.  Pydoop has several
advantages [#pydoop]_ over Hadoop's built-in solutions for Python
programming, i.e., Hadoop Streaming and Jython: being a CPython
package, it allows you to access all standard library and third party
modules, some of which may not be available for other Python
implementations -- e.g., `SciPy <http://www.scipy.org>`_; in
addition, Pydoop provides a Python HDFS API which, to the best of our
knowledge, is not available in other solutions.


.. toctree::
   :maxdepth: 2

   pydoop_script
   mapred_api


.. note::

  TODO: the following docs must be moved to their own sections


High-level HDFS API
+++++++++++++++++++

Pydoop includes a high-level :ref:`HDFS API <hdfs-api>` that
simplifies common tasks such as copying files and directories,
navigating through the file system, etc.  Here is a brief snippet that
shows some of these functionalities:

.. code-block:: python

  >>> import pydoop.hdfs as hdfs
  >>> hdfs.mkdir("test")
  >>> hdfs.dump("hello", "test/hello.txt")
  >>> hdfs.cp("test", "test.copy")

See the :ref:`tutorial <hdfs-api-examples>` for more examples.


.. rubric:: Footnotes

.. [#pydoop] Simone Leo, Gianluigi Zanetti. `Pydoop: a Python
   MapReduce and HDFS API for Hadoop.
   <http://dx.doi.org/10.1145/1851476.1851594>`_, Proceedings Of The
   19th ACM International Symposium On High Performance Distributed
   Computing, page 819--825, 2010
