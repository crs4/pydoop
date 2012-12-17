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

Upload your input data to HDFS:

  hadoop fs -put input hdfs_input

Run the pydoop script program:

  pydoop script script.py hdfs_input hdfs_output


Examples
--------

The following examples show how to use Pydoop Script for common
problems.  More examples can be found in the
``examples/pydoop_script`` subdirectory of Pydoop's source
distribution root.  A `more detailed guide on Pydoop Script
<pydoop_script_guide>` is also available.

Word Count
++++++++++

.. code-block:: python

  def mapper(_, text, writer):
    for word in text.split():
      writer.emit(word, 1)

  def reducer(word, icounts, writer):
    writer.emit(word, sum(map(int, icounts)))

Notice that in the reducer we had to convert the values to ``int``
since all data come in as strings.

Word Count with Total Number of Words
+++++++++++++++++++++++++++++++++++++

Suppose that we want to count the occurrence of specific words, like
in the example above, but we also want the total number of words.  For
this last "global" count we can use Hadoop counters:

.. code-block:: python

  def mapper(_, text, writer):
    wordlist = text.split()
    for word in wordlist:
      writer.emit(word, 1)
    writer.count("num words", len(wordlist))

  def reducer(word, count, writer):
    writer.emit(word, sum(map(int, count)))

The counter value will show on the JobTracker's job page and will be
present in the job logs.

Lower Case
++++++++++

To convert some text to lower case, create a module ``lowercase.py``:

.. code-block:: python

  def mapper(_, text, writer):
    writer.emit("", text.lower())

This is a map-only job, so we set the number of reducers to 0.  To
avoid leading tabs in our results, we also want an empty separator for
output key-value pairs: this is done via the ``-t`` option::

  pydoop script --num-reducers 0 -t '' lowercase.py hdfs_input hdfs_output

Job Parameters
++++++++++++++

Suppose you want to select all lines containing a substring to be
given at run time.  Create a module ``grep.py``:

.. code-block:: python

  def mapper(_, text, writer, conf):  # notice the fourth 'conf' argument
    if text.find(conf['grep-expression']) >= 0:
      writer.emit("", text)

Job parameters, like in ``hadoop pipes``, are passed via the -D
option::

  pydoop script --num-reducers 0 -t '' -D grep-expression=my_substring \
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
:ref:`full Pydoop API <api-docs>`.
