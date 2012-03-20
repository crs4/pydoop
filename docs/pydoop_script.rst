.. _pydoop_script:

Pydoop Script
=========================

`pydoop_script` is a helper command that for the simpler use cases hides all
the details of running a regular Pydoop job.  Basically, it reduces running
a simple text-processing Pydoop job to writing two
Python functions in a module (or one if you don't need a reducer) and running
them like this::

  pydoop_script myscript.py hdfs_input hdfs_output

The rest is magic.  Perhaps ``pydoop_script`` is best explained with a couple of
short examples.

Examples
----------------

Lower case
....................

To convert some text to lower case, create a module `lowercase.py`::

  def mapper(k,value, writer):
    writer.emit("", value.lower())

Now run it over your data::

  pydoop_script --num-reducers 0 --kv-separator '' lowercase.py hdfs_input hdfs_output


Word counting
................

Write your map and reduce functions in `wordcount.py`::

  def mapper(k, text, writer):
    for word in text.split():
      writer.emit(word, 1)

  def reducer(word, count, writer):
    writer.emit(word, sum(map(int, count)))

Notice that in the reducer we had to convert the values to `int` since all data
comes in as strings.

Run the example::

  pydoop_script wordcount.py hdfs_input hdfs_output


Word count with total number of words
..........................................

Suppose that we want to count the occurrence of specific words, like the example
above, but in addition we also want to count the total number of words.  For
this last "global" count we can use Hadoop counters.

Here's our code in `wordcount_with_total.py`::

  def mapper(k, text, writer):
    wordlist = text.split()
    for word in wordlist:
      writer.emit(word, 1)
    writer.count("num words", len(wordlist))

  def reducer(word, count, writer):
    writer.emit(word, sum(map(int, count)))

Then run it::

  pydoop_script wordcount_with_total.py hdfs_input hdfs_output

The counter value will show on the JobTracker's job page and will be present in
the job logs.


Measuring nucleic acid composition of a DNA sample
.....................................................

This is a more domain-specific problem.  We have some DNA sequencing data in the
text, tab-delimited SAM format.  We'd like to calculate the nucleotide
composition of the sequenced sample.

Our module, `nukes.py`::

  def mapper(k, samrecord, writer):
    seq = samrecord.split("\t", 10)[9] # extract the DNA sequence
    for c in seq: # for each base
      writer.emit(c, 1)
    writer.count("bases", len(seq)) # count all the bases

  def reducer(key, ivalue, writer):
    writer.emit(key, sum(map(int, ivalue)))

Run it::

  pydoop_script nukes.py hdfs_input hdfs_output


Applicability
------------------------

`pydoop_script` makes it easy to solve simple problems.  It makes it feasible to
write simple (even throw-away) scripts to perform simple manipulations or analyses on
your data, especially if it's text-based.

If you can specify your algorithm in two simple functions that have no state
or have a simple state that can be stored in module variables, then you can
consider using ``pydoop_script``.

If you need something more sophisticated, then consider using the full Pydoop
API or the native Hadoop Java API.


Usage
---------------

::

  pydoop_script MODULE INPUT OUTPUT


``MODULE`` is the file (on your local file system) containing your map and reduce
functions, in Python.

``INPUT`` and ``OUTPUT`` are HDFS paths, the former pointing to your input data and
the latter to your job's output directory.

Command line options supported by ``pydoop_script``.

====== ======================= =================================================================
Short  Long                     Meaning
====== ======================= =================================================================
-h,    --help                   show this help message and exit
-m     --map-fn                 Name of map function within module (default: mapper)
-r     --reduce-fn              Name of reduce function within module (default: reducer)
-t     --kv-separator           Key-value separator string in final output (default:
                                <tab> character)
       --num-reducers           Number of reduce tasks. Specify 0 to only perform map
                                phase (default: 3 * num task trackers).
-D                              Set a property value, such as
                                -D mapred.compress.map.output=true
====== ======================= =================================================================


Generic Hadoop options
.........................

In addition to the options listed above, you can pass any of the generic Hadoop
options to pydoop_script, but you must pass them **after the pydoop_script
options listed above**.

============================== =================================================
-conf <configuration file>     specify an application configuration file
-fs <local|namenode:port>      specify a namenode
-jt <local|jobtracker:port>    specify a job tracker
-files <list of files>         comma-separated files to be copied to the map
                               reduce cluster
-libjars <list of jars>        comma-separated jar files to include in the
                               classpath
-archives <list of archives>   comma-separated archives to be unarchived on the
                               compute machines
============================== =================================================

Example: word count with stop words
"""""""""""""""""""""""""""""""""""""

Here is the word count example modified to ignore stop words.  The stop words
are identified in a file that is distributed to all the nodes using the standard
Hadoop ``-files`` option.

Code::

  # load stop word dictionary
  stop = dict()
  with open('stop_words.txt') as f:
    for line in f.xreadlines():
      stop[line.rstrip('\n')] = None

  # map and reduce 
  def mapper(k,v,writer):
    for word in v.rstrip('\n').split():
      if stop.has_key(word):
        writer.count("stop words", 1)
      else:
        writer.emit(word,1)
  
  def reducer(word, icounts, writer):
    writer.emit(word, sum(map(int, icounts)))

Command line::

  pydoop_script  word_count.py alice.txt wc -files stop_words.txt

While this script works, it has the obvious weakness of loading the stop words
list even when executing the reducer (since it's loaded as soon as we import the
module).  If this inconvenience is a concern, we could solve the issue by
triggering the loading from the ``mapper`` function, or by writing a full Pydoop
application which would give us all the control we need to only load the list
when required.



Writing your map and reduce functions
-----------------------------------------

In this section we assume you'll be using the default TextInputFormat and
TextOutputFormat record reader/writer.  You may select a different input or output
format by configuring the appropriate Hadoop properties.


mapper
........

The ``mapper`` function in your module will be called for each record in your input
data.  It receives 3 parameters:

#. key
#. value
#. writer object

key:
  You can ignore the key value.

value:
  This is the line of text to be processed.

writer:
  A Python object to write output and count values.  It has two methods:  ``emit(k,v)`` and ``count(what,
  how_many)``.


reducer
............

The ``reducer`` function will be called for each unique key value produced by your
map function.  It also receives 3 parameters:

#. key
#. values iterable
#. writer object

key:
  The key produced by your map function

values iterable:
  Iterate over this parameter to see all the values emitted for this key.

writer:
  A writer object identical to the one given to the map function


The key and value your emit from your reducer will be joined by the key-value
separator and written to the final output.  You may customize the key-value
separator with the ``--kv-separator`` command line argument.



Writer object
.................

The writer object given as the third parameter to both ``mapper`` and
``reducer`` functions has two methods:  ``emit(k,v)`` and ``count(what, how_many)``.

Call ``emit(k,v)`` to write a key-value pair (k,v) to the framework.

Call ``count(what, how_many)`` to add ``how_many`` to the counter named
``what``.  If the counter doesn't already exist it will be created dynamically.


Naming your functions
........................

If you'd like to give your map and reduce functions names different from
``mapper`` and ``reducer``, you may do so but you must tell ``pydoop_script``.
Use the ``--map-fn`` and ``--reduce-fn`` command line arguments to select your
customized names.


Map-only jobs
................

You may have a program that doesn't use a reduce function.  Specify
``--num-reducers 0`` on the command line and your map output will be written
directly to file.  In this case, you map output will go directly to the output
formatter and be written to your final output, separated by the key-value
separator.
