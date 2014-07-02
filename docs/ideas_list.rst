.. _ideas_list:

Ideas List
==========

This page contains a list of future development ideas which we
currently can't pursue due to lack of resources.  They are accessible
to developers with a good knowledge of Python and Java, possibly in
addition to some degree of familiarity with `Hadoop
<http://hadoop.apache.org>`_.  If you find anything you'd like to work
on, just `fork us on GitHub <https://github.com/crs4/pydoop/fork>`_
and start coding!

Introduction
------------

Pydoop provides a Python MapReduce and HDFS API for Hadoop. Its
current implementation relies on extension modules that wrap Hadoop
Pipes (C++) and libhdfs (C) using `Boost.Python
<http://www.boost.org/doc/libs/1_55_0/libs/python/doc>`_.  While this
solution has been effective, it has several important drawbacks.

First, the system incurs the overhead of traversing two language
boundaries: Java to C++ to Python.  This design entails a significant
overhead in type conversion and data copying; moreover, it limits the
types of Python components that can be plugged into the framework and
it complicates the interaction of the client Python code with the
Hadoop framework since much of the API is not exposed through these
levels.

Furthermore, the current design increases the maintenance effort
required to keep Pydoop working with new Hadoop versions.  In fact,
Hadoop Pipes and libhdfs are not maintained with the same level of
care as the rest of the Hadoop code base.  As such, Pydoop needs to
patch and rebuild these components as part of its installation, thus
requiring the Pydoop developers to generate new patches for each new
supported Hadoop version. In addition, building and installing the
binary libraries themselves requires the support if the cluster
administrator which -- especially when summed with the Boost.Python
dependency -- makes installing Pydoop a complicated matter.

For these reasons, we would like to gradually make Pydoop “C++ free”,
with a Python implementation that interfaces directly with the
Java-based Hadoop framework.  Interested in helping out?  The
following ideas take Pydoop in this direction, one step at a time.

New Python HDFS library
-----------------------

libhdfs is a JNI-based C API for the Hadoop Distributed File System
(HDFS). It provides a simple subset of HDFS APIs to manipulate and
access files and the filesystem structure.  Pydoop wraps libhdfs in
Python, but in doing so it introduces the aforementioned maintenance
and overhead issues.

We would like to change all this and see a new implementation that
wraps the Java HDFS API directly with Python -- using a framework like
`JPype <http://jpype.sourceforge.net/>`_. Such a direct wrapping
should eliminate the main issues highlighted with the current
implementation and allow us to expose the entire functionality
provided by the Java-based API to Python code.

**Knowledge prerequisites:** Python, Java, basic knowledge of C/C++

**Skill level:** advanced

Alternative implementation of Hadoop Pipes
------------------------------------------

Hadoop Pipes is the C++ interface to Hadoop MapReduce.  Pydoop’s
MapReduce API is currently implemented as a Boost.Python wrapper for
Hadoop Pipes’ C++ API . Hadoop Pipes uses a Java client that launches
an executable and interacts with it through a dedicated protocol. This
solution has some drawbacks.  For instance, piping data to the worker
process incurs data (de)serialization and copying overhead, in
addition to making it difficult to reuse standard InputFormat and
OutputFormat implementations and limiting the framework components
that can be overridden with Python code to the bare minimum.

We would like to have a better solution that implements a custom Java
client that communicates directly with Python to improve performance
and allow for more pythonic code. Frameworks like JPype would allow
pure Python customization of a wider array of MapReduce components.

**Knowledge prerequisites:** Python, Java, basic knowledge of C/C++

**Skill level:** advanced

Customizing InputSplits in Python
---------------------------------

In the Hadoop workflow, one of the first steps the framework performs
when running a job is to look over the input data and decide how to
split it among a number of independent processing tasks. Within Hadoop
this functionality is implemented in an InputFormat class. Overriding
this class allows you to customize how input files are split --
information which is captured in a list of InputSplit objects.

At the moment it is not possible to implement your own InputFormat in
Python and plug it into the framework; Java must be used instead. The
idea is to add the feature to Pydoop so that developers could
implement custom input splitting in pure Python, which would
complement the currently available possibility to write a custom
record reader in Python -- i.e., a full input data format
implementation in Python.

**Knowledge prerequisites:** Python, Java

**Skill level:** medium

Python 3 porting
----------------

Pydoop currently runs on Python 2.7 (and 2.6, with some backports). It
would be nice to make it available to Python 3 users.

**Knowledge prerequisites:** Python 2 (Python 3 is a plus)

**Skill level:** basic
