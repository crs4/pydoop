.. _news:

News
==============================

New in this release
--------------------------

Updated for Hadoop 1.0
++++++++++++++++++++++++++

Pydoop now works with Hadoop 1.0 (we've tested with Hadoop 1.0.0)


Support for multiple Hadoop versions
++++++++++++++++++++++++++++++++++++++

Multiple versions of Hadoop can now be supported by the same installation of 
Pydoop.  Once you've built the module for each version of Hadoop you'd like to
use (see the :ref:`installation <installation>` page, and in particular the
section on :ref:`multiple Hadoop versions <multiple_hadoop_versions>`), the 
runtime will automatically and transparently import the right one for the 
version of Hadoop selected by you HADOOP_HOME variable.  This feature should 
make migration between Hadoop versions easier for our users.


Easy Pydoop scripting
+++++++++++++++++++++++

We have added a new executable :ref:`pydoop_script <pydoop_script>` to the 
Pydoop distribution to make it trivially
simple to write shorts scripts for simple problems.  See the
:ref:`pydoop_script <pydoop_script>` page for details.


Python 2.7 required
++++++++++++++++++++++++

Pydoop now requires Pydoop 2.7.  We now use `importlib` which was introduced in
Pydoop 2.7.



Support for Hadoop 0.21 has been dropped
+++++++++++++++++++++++++++++++++++++++++++

We have dropped support for the 0.21 branch of Hadoop.
