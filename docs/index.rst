.. Pydoop documentation master file, created by
   sphinx-quickstart on Sun Jun 20 17:06:55 2010.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Pydoop |release| Documentation
==============================

Welcome to Pydoop.  Pydoop is a package that provides a Python API Hadoop
MapReduce and HDFS.  As opposed to other solutions for Hadoop Python 
programming, Pydoop uses the CPython interpreter and as such allows you to
access all the regular Python modules, some of which may not be available for
other Python interpreters---e.g. NumPy.  Pydoop is based on Hadoop pipes, which
is slightly faster the Hadoop streaming.  In addition, Pydoop provides a native
Python HDFS API which, to the best of our knowledge, is not available in other
solutions.


Easy Hadoop scripting
+++++++++++++++++++++++++

In addition to its complete MapReduce API, Pydoop also provides a solution for
easy Hadoop scripting which allows you to work in a way similar to 
`Dumbo <https://github.com/klbostee/dumbo>`_.  This mechanism lowers the
programming effort to the point that you may start finding yourself writing 
simple 3-line throw-away Hadoop scripts!  See the :ref:`pydoop_script` page for
details.


Contents:
==========================

.. toctree::
   :maxdepth: 2

   news
   installation
   pydoop_script
   running_pydoop_applications
   api_docs/index
   examples/index
   self_contained
   for_dumbo_users

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

