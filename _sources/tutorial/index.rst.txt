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
   hdfs_api
   mapred_api


.. rubric:: Footnotes

.. [#pydoop] Simone Leo, Gianluigi Zanetti. `Pydoop: a Python
   MapReduce and HDFS API for Hadoop.
   <http://dx.doi.org/10.1145/1851476.1851594>`_, Proceedings Of The
   19th ACM International Symposium On High Performance Distributed
   Computing, page 819--825, 2010
