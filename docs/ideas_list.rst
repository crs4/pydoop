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
