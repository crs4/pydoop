.. _pydoop_script_tutorial:

Easy Hadoop Scripting with Pydoop Script
========================================

Pydoop Script is the easiest way to write simple MapReduce programs
for Hadoop.  With Pydoop Script, your code focuses on the core of the
MapReduce model: the mapper and reducer functions.


Writing and Running Scripts
---------------------------

Write a ``script.py`` Python module that contains the mapper and
reducer functions:

.. code-block:: python

  def mapper(input_key, input_value, writer):
      # your computation here
      writer.emit(intermediate_key, intermediate_value)

  def reducer(intermediate_key, value_iterator, writer):
      # your computation here
      writer.emit(output_key, output_value)

The program can be run as follows::

  pydoop script script.py hdfs_input hdfs_output


Examples
--------

The following examples show how to use Pydoop Script for common
problems.  More examples can be found in the
``examples/pydoop_script`` subdirectory of Pydoop's source
distribution root.  The :ref:`Pydoop Script Guide
<pydoop_script_guide>` contains more detailed information on writing
and running programs.


.. _word_count:

Word Count
++++++++++

The word count example can be considered as the "hello world" of
MapReduce.  A simple application that counts the occurrence of each
word in a set of text files, it is included in both the original
MapReduce paper [#]_ and in the Hadoop documentation as a MapReduce
programming tutorial.  The Pydoop Script implementation requires only
five lines of code:

.. literalinclude:: ../../examples/pydoop_script/scripts/wordcount.py
   :language: python
   :start-after: DOCS_INCLUDE_START

A few more lines allow to set a combiner for local aggregation:

.. literalinclude:: ../../examples/pydoop_script/scripts/wc_combiner.py
   :language: python
   :start-after: DOCS_INCLUDE_START

Run the example with::

  pydoop script -c combiner wordcount.py hdfs_input hdfs_output

Note that we need to explicitly set the ``-c`` flag to activate the
combiner.  By default, no combiner is called.

One thing to remember is that the current Hadoop Pipes architecture
runs the combiner under the hood of the executable run by ``pipes``,
so it does not update the combiner counters of the general Hadoop
framework.  Thus, if you run the above script, you'll get a value of 0
for "Combine input/output records" in the "Map-Reduce Framework"
group, but the "combiner calls" counter should be updated correctly.


Map-only Jobs and Output Separators
+++++++++++++++++++++++++++++++++++

Suppose we want to convert all input text to lower case. All we need to do is read each input line, convert it to lower case and emit it (for instance, as the output value). Since there is no aggregation involved, we don't need a reducer:

.. literalinclude:: ../../examples/pydoop_script/scripts/lowercase.py
   :language: python
   :start-after: DOCS_INCLUDE_START

The only problem with the above code is that, by default, each output key-value pair is written as tab-separated, which would lead to each output line having a leading tab character that's not found in the original input (note that we'd get a *trailing* tab if we emitted each record as the output key instead). We can turn off the reduce phase and get an empty separator for output key-value pairs by submitting the job with the following options::

  pydoop script --num-reducers 0 -t '' lowercase.py hdfs_input hdfs_output


Custom Parameters
+++++++++++++++++

Suppose we want to select all lines containing a substring to be given at run time (distributed grep). As in the previous example, we can do this with a map-only job (read each input line and emit it if it contains the substring), but we need a way for the user of our application to specify the substring to be matched. This can be done by adding a fourth argument to the mapper function:

.. literalinclude:: ../../examples/pydoop_script/scripts/grep.py
   :language: python
   :start-after: DOCS_INCLUDE_START

In this case, Pydoop Script passes the Hadoop job configuration to the ``mapper`` function as a dictionary via the fourth argument. Moreover, just like Hadoop tools (e.g., ``hadoop pipes``), Pydoop Script allows to set additional configuration parameters via ``-D key=value``. To search for "hello", for instance, we can run the application as::

  pydoop script --num-reducers 0 -t '' -D grep-expression=hello \
    grep.py hdfs_input hdfs_output


Applicability
-------------

Pydoop Script makes it easy to solve simple problems.  It makes it
feasible to write simple (even throw-away) scripts to perform simple
manipulations or analyses on your data, especially if it's text-based.

If you can specify your algorithm in two simple functions that have no
state or have a simple state that can be stored in module variables,
then you can consider using Pydoop Script.

If you need something more sophisticated, then consider using the
:ref:`full Pydoop API <api_tutorial>`.


.. rubric:: Footnotes

.. [#] J. Dean and S. Ghemawat, *MapReduce: simplified data processing
       on large clusters*, in OSDI '04: 6th Symposium on Operating
       Systems Design and Implementation, 2004
