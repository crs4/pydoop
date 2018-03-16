.. _pydoop_script_guide:

Pydoop Script User Guide
========================

Pydoop Script is the easiest way to write simple MapReduce programs
for Hadoop.  With Pydoop Script, you only need to write a map and/or a reduce
functions and the system will take care of the rest.

For a full explanation please see the :ref:`tutorial <pydoop_script_tutorial>`.


Command Line Tool
-----------------

In the simplest case, Pydoop Script is invoked as::

  pydoop script MODULE INPUT OUTPUT

where ``MODULE`` is the file (on your local file system) containing
your map and reduce functions, in Python, while ``INPUT`` and
``OUTPUT`` are, respectively, the HDFS paths of your input data and
your job's output directory.

Options are shown in the following table.

.. include:: pydoop_script_options.rst


Example: Word Count with Stop Words
+++++++++++++++++++++++++++++++++++

Here is the word count example modified to ignore stop words from a
file that is distributed to all the nodes via the Hadoop distributed
cache:

.. literalinclude:: ../examples/pydoop_script/scripts/wordcount_sw.py
   :language: python
   :start-after: DOCS_INCLUDE_START

To execute the above script, save it to a ``wc.py`` file and run::

  pydoop script wc.py hdfs_input hdfs_output --upload-file-to-cache stop_words.txt

where ``stop_words.txt`` is a text file that contains the stop words,
one per line.

While this script works, it has the obvious weakness of loading the
stop words list even when executing the reducer (since it's loaded as
soon as we import the module).  If this inconvenience is a concern, we
could solve the issue by triggering the loading from the ``mapper``
function, or by writing a :ref:`full Pydoop application <api_tutorial>`
which would give us all the control we need to only load the list when
required.

Writing your Map and Reduce Functions
-------------------------------------

In this section we assume you'll be using the default ``TextInputFormat``
and ``TextOutputFormat``.

Mapper
++++++

The ``mapper`` function in your module will be called for each record
in your input data.  It receives 3 parameters:

#. key: the byte offset with respect to the current input file. In most cases,
   you can ignore it;
#. value: the line of text to be processed;
#. writer object: a Python object to write output and count values (see below);
#. optionally, a job conf object from which to fetch configuration
   property values (see `Accessing Parameters`_ below).

Combiner
++++++++

The ``combiner`` function will be called for each unique key-value pair
produced by your map function.  It also receives 3 parameters:

#. key: the key produced by your map function
#. values iterable: iterate over this parameter to see all the values emitted
   for the current key
#. writer object: a writer object identical to the one given to the
   map function
#. optionally, a job conf object, identical to the one given to the
   map function.

The key-value pair emitted by your combiner will be piped to the reducer.

Reducer
+++++++

The ``reducer`` function will be called for each unique key-value pair
produced by your map function.  It also receives 3 parameters:

#. key: the key produced by your map function;
#. values iterable: iterate over this parameter to traverse all the
   values emitted for the current key;
#. writer object: this is identical to the one given to the map function;
#. optionally, a job conf object, identical to the one given to the
   map function.

The key-value pair emitted by your reducer will be joined by the
key-value separator specified with the ``--kv-separator`` option
(a tab character by default).


Writer Object
+++++++++++++

The writer object given as the third parameter to both the ``mapper``
and ``reducer`` functions has the following methods:

* ``emit(k, v)``: pass a ``(k, v)`` key-value pair to the framework;
* ``count(what, how_many)``: add ``how_many`` to the counter named
  ``what``.  If the counter doesn't already exist, it will be created
  dynamically;
* ``status(msg)``: update the task status to ``msg``;
* ``progress()``: mark your task as having made progress without changing
  the status message.

The latter two methods are useful for keeping your task alive in cases
where the amount of computation to be done for a single record might
exceed Hadoop's timeout interval: Hadoop kills a task after a number
of milliseconds set through the ``mapreduce.task.timeout`` property --
which defaults to 600000, i.e., 10 minutes -- if it neither reads an
input, writes an output, nor updates its status string.


Accessing Parameters
++++++++++++++++++++

Pydoop Script lets you access the values of your job configuration
properties through a dict-like :class:`~pydoop.mapreduce.api.JobConf`
object, which gets passed as the fourth (optional) parameter to your
functions.


Naming your Functions
+++++++++++++++++++++

If you'd like to give your map and reduce functions names different
from ``mapper`` and ``reducer``, you may do so, but you must tell the
script tool.  Use the ``--map-fn`` and ``--reduce-fn`` command line
arguments to select your customized names.  Combiner functions can only
be assigned by explicitly setting the ``--combine-fn`` flag.


Map-only Jobs
+++++++++++++

You may have a program that doesn't use a reduce function.  Specify
``--num-reducers 0`` on the command line and your map output will be
written directly to file.  In this case, your map output will go
directly to the output formatter and be written to your final output,
separated by the key-value separator.
