.. _pydoop_script:

Pydoop Script
=============

Pydoop includes a command line tool that, for the simpler use cases,
hides all the details of running a regular Pydoop job.  Basically, it
reduces running a simple text-processing Pydoop job to writing two
Python functions (or one if you don't need a reducer) in a module  and
running them like this::

  pydoop script myscript.py hdfs_input hdfs_output

The rest is magic.


Tutorial
--------

Lower Case
..........

To convert some text to lower case, create a module ``lowercase.py``:

.. code-block:: python

  def mapper(_, value, writer):
    writer.emit("", value.lower())

Now run it over your data::

  pydoop script --num-reducers 0 -t '' lowercase.py hdfs_input hdfs_output

Note that we want a map-only job, so we set the number of reducers to 0.


Word Counting
.............

Write your map and reduce functions in ``wordcount.py``:

.. code-block:: python

  def mapper(_, text, writer):
    for word in text.split():
      writer.emit(word, 1)

  def reducer(word, count, writer):
    writer.emit(word, sum(map(int, count)))

Notice that in the reducer we had to convert the values to ``int``
since all data come in as strings.

Run the example::

  pydoop script wordcount.py hdfs_input hdfs_output


Word Count with Total Number of Words
.....................................

Suppose that we want to count the occurrence of specific words, like the example
above, but in addition we also want to count the total number of words.  For
this last "global" count we can use Hadoop counters.

Here's our code in ``wordcount_with_total.py``:

.. code-block:: python

  def mapper(_, text, writer):
    wordlist = text.split()
    for word in wordlist:
      writer.emit(word, 1)
    writer.count("num words", len(wordlist))

  def reducer(word, count, writer):
    writer.emit(word, sum(map(int, count)))

Then run it::

  pydoop script wordcount_with_total.py hdfs_input hdfs_output

The counter value will show on the JobTracker's job page and will be present in
the job logs.


Measuring Nucleic Acid Composition of a DNA Sample
..................................................

This is a more domain-specific problem.  We have some DNA sequencing
data in `SAM format <http://samtools.sourceforge.net>`_.  We'd like to
calculate the nucleotide composition of the sequenced sample.

Our module, ``base_histogram.py``:

.. code-block:: python

  def mapper(_, samrecord, writer):
    seq = samrecord.split("\t", 10)[9] # extract the DNA sequence
    for c in seq:
      writer.emit(c, 1)
    writer.count("bases", len(seq)) # count all the bases

  def reducer(key, ivalue, writer):
    writer.emit(key, sum(map(int, ivalue)))

Run it::

  pydoop script base_histogram.py hdfs_input hdfs_output


Applicability
-------------

Pydoop Script makes it easy to solve simple problems.  It makes it
feasible to write simple (even throw-away) scripts to perform simple
manipulations or analyses on your data, especially if it's text-based.

If you can specify your algorithm in two simple functions that have no state
or have a simple state that can be stored in module variables, then you can
consider using Pydoop Script.

If you need something more sophisticated, then consider using the full Pydoop
API.


Usage
-----

::

  pydoop script MODULE INPUT OUTPUT

``MODULE`` is the file (on your local file system) containing your map
and reduce functions, in Python.

``INPUT`` and ``OUTPUT`` are HDFS paths, the former pointing to your
input data and the latter to your job's output directory.

Command line options are shown in the following table.

+--------+--------------------+-----------------------------------------------+
| Short  | Long               | Meaning                                       |
+========+====================+===============================================+
| ``-m`` | ``--map-fn``       | Name of map function within module (default:  |
|        |                    | mapper)                                       |
+--------+--------------------+-----------------------------------------------+
| ``-r`` | ``--reduce-fn``    | Name of reduce function within module         |
|        |                    | (default: reducer)                            |
+--------+--------------------+-----------------------------------------------+
| ``-t`` | ``--kv-separator`` | Key-value separator string in final output    |
|        |                    | (default: <tab> character)                    |
+--------+--------------------+-----------------------------------------------+
|        | ``--num-reducers`` | Number of reduce tasks. Specify 0 to only     |
|        |                    | perform map phase (default: 3 * num task      |
|        |                    | trackers)                                     |
+--------+--------------------+-----------------------------------------------+
| ``-D`` |                    | Set a property value, such as                 |
|        |                    | -D mapred.compress.map.output=true            |
+--------+--------------------+-----------------------------------------------+


Generic Hadoop options
......................

In addition to the options listed above, you can pass any of the generic Hadoop
options to the script tool, but you must pass them **after the pydoop script
options listed above**.

================================ ==============================================
``-conf <configuration file>``   specify an application configuration file
``-fs <local|namenode:port>``    specify a namenode
``-jt <local|jobtracker:port>``  specify a job tracker
``-files <list of files>``       comma-separated files to be copied to the map
                                 reduce cluster
``-libjars <list of jars>``      comma-separated jar files to include in the
                                 classpath
``-archives <list of archives>`` comma-separated archives to be unarchived on
                                 the compute machines
================================ ==============================================

Example: Word Count with Stop Words
"""""""""""""""""""""""""""""""""""

Here is the word count example modified to ignore stop words.  The stop words
are identified in a file that is distributed to all the nodes using the standard
Hadoop ``-files`` option.

Code:

.. code-block:: python

  with open('stop_words.txt') as f:
    STOP_WORDS = frozenset(l.strip() for l in f if not l.isspace())

  def mapper(_, v, writer):
    for word in v.split():
      if word in STOP_WORDS:
        writer.count("STOP_WORDS", 1)
      else:
        writer.emit(word, 1)

  def reducer(word, icounts, writer):
    writer.emit(word, sum(map(int, icounts)))

Command line::

  pydoop script word_count.py alice.txt hdfs_output -files stop_words.txt

While this script works, it has the obvious weakness of loading the stop words
list even when executing the reducer (since it's loaded as soon as we import the
module).  If this inconvenience is a concern, we could solve the issue by
triggering the loading from the ``mapper`` function, or by writing a full Pydoop
application which would give us all the control we need to only load the list
when required.


Writing your Map and Reduce Functions
-------------------------------------

In this section we assume you'll be using the default TextInputFormat
and TextOutputFormat record reader/writer.  You may select a different
input or output format by configuring the appropriate Hadoop
properties.


Mapper
......

The ``mapper`` function in your module will be called for each record
in your input data.  It receives 3 parameters:

#. key: the byte offset with respect to the current input file. In most cases,
   you can ignore it
#. value: the line of text to be processed
#. writer object: a Python object to write output and count values (see below)
#. optionally, a jc_wrapper conf object: a Python object from which to fetch
   configuration property values (see `Accessing Parameters` below).


Reducer
.......

The ``reducer`` function will be called for each unique key value
produced by your map function.  It also receives 3 parameters:

#. key: the key produced by your map function
#. values iterable: iterate over this parameter to see all the values emitted
   for the current key
#. writer object: a writer object identical to the one given to the map function
#. optionally, a jc_wrapper conf object: a Python object from which to fetch
   configuration property values (see `Accessing Parameters` below).

The key and value your emit from your reducer will be joined by the key-value
separator and written to the final output.  You may customize the key-value
separator with the ``--kv-separator`` command line argument.


Writer Object
.............

The writer object given as the third parameter to both ``mapper`` and
``reducer`` functions has the following methods:

* ``emit(k, v)``: pass a ``(k, v)`` key-value pair to the framework
* ``count(what, how_many)``: add ``how_many`` to the counter named
  ``what``.  If the counter doesn't already exist, it will be created
  dynamically
* ``status(msg)``: update the task status to ``msg``
* ``progress()``: mark your task as having made progress without changing
  the status message

The latter two methods are useful for keeping your task alive in cases
where the amount of computation to be done for a single record might
exceed Hadoop's timeout interval: Hadoop kills a task after a number
of milliseconds set through the ``mapred.task.timeout`` property --
which defaults to 600000, i.e., 10 minutes -- if it neither reads an
input, writes an output, nor updates its status string.


Accessing Parameters
......................

If desired, Pydoop Script lets you access the values of your programs configuration
properties through a dict-like configuration object, which gets passed as the
fourth parameter to your functions (if available).  To see the methods
available check out the :ref:`api<pydoop-jc>`.

Naming your Functions
.....................

If you'd like to give your map and reduce functions names different from
``mapper`` and ``reducer``, you may do so, but you must tell the script tool.
Use the ``--map-fn`` and ``--reduce-fn`` command line arguments to select your
customized names.


Map-only Jobs
.............

You may have a program that doesn't use a reduce function.  Specify
``--num-reducers 0`` on the command line and your map output will be written
directly to file.  In this case, you map output will go directly to the output
formatter and be written to your final output, separated by the key-value
separator.
