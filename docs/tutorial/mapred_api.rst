.. _api_tutorial:

Writing Full-Featured Applications
==================================

While :ref:`Pydoop Script <pydoop_script_tutorial>` allows to solve
many problems with minimal programming effort, some tasks require a
broader set of features.  If your data is not structured into simple
text lines, for instance, you may need to write a record reader; if
you need to change the way intermediate keys are assigned to reducers,
you have to write your own partitioner.  These components are
accessible via the Pydoop MapReduce API, a Python wrapper of the `C++
one <http://wiki.apache.org/hadoop/C%2B%2BWordCount>`_.

The rest of this section serves as an introduction to MapReduce
programming with Pydoop; the :ref:`API reference <mr_api>` has
all the details.

Mappers and Reducers
--------------------

The Pydoop API is object-oriented: the application developer writes a
:class:`~pydoop.pipes.Mapper` class, whose core job is performed by
the :meth:`~pydoop.pipes.Mapper.map` method, and a
:class:`~pydoop.pipes.Reducer` class that processes data via the
:meth:`~pydoop.pipes.Reducer.reduce` method.  The following snippet
shows how to write the mapper and reducer for the :ref:`word count
<word_count>` problem:

.. code-block:: python

  #!/usr/bin/env python
  import pydoop.pipes as pp

  class Mapper(pp.Mapper):

    def map(self, context):
      words = context.getInputValue().split()
      for w in words:
        context.emit(w, "1")

  class Reducer(pp.Reducer):

    def reduce(self, context):
      s = 0
      while context.nextValue():
        s += int(context.getInputValue())
      context.emit(context.getInputKey(), str(s))

  if __name__ == "__main__":
    pp.runTask(pp.Factory(Mapper, Reducer))

The mapper is instantiated by the Hadoop framework that, for each
input record, calls the map method passing a ``context`` object to it.
The context serves as a communication interface between the framework
and the application: in the map method, it is used to get the current
key (not used in the above example) and value, and to emit (send back
to the framework) intermediate key-value pairs.  The reducer works in
a similar way, the main difference being the fact that the reduce
method gets a set of values for each key.  The context has several
other functions that we will explore later.

To run the above program, save it to a "wordcount" file and execute::

  hadoop fs -put wordcount{,}
  hadoop fs -put input{,}
  hadoop pipes \
    -D hadoop.pipes.java.recordreader=true \
    -D hadoop.pipes.java.recordwriter=true \
    -program wordcount -input input -output output

See the section on :ref:`running Pydoop programs<running_apps>` for
more details.  Source code for the word count example is located under
``examples/wordcount`` in the Pydoop distribution.

Counters and Status Updates
---------------------------

Hadoop features application-wide counters that can be set and
incremented by developers.  Status updates are arbitrary text messages
sent to the framework: these are especially useful in cases where the
computation associated with a single input record can take a
considerable amount of time, since Hadoop kills tasks that read no
input, write no output and do not update the status within a
configurable amount of time (ten minutes by default).

The following snippet shows how to use counters and update the status:

.. code-block:: python

  import pydoop.pipes as pp

  class Mapper(pp.Mapper):

    def __init__(self, context):
      super(Mapper, self).__init__(context)
      context.setStatus("initializing mapper")
      self.input_words = context.getCounter("WC", "INPUT_WORDS")

    def map(self, context):
      k = context.getInputKey()
      words = context.getInputValue().split()
      for w in words:
        context.emit(w, "1")
      context.incrementCounter(self.input_words, len(words))

  class Reducer(pp.Reducer):

    def __init__(self, context):
      super(Reducer, self).__init__(context)
      context.setStatus("initializing reducer")
      self.output_words = context.getCounter("WC", "OUTPUT_WORDS")

    def reduce(self, context):
      s = 0
      while context.nextValue():
        s += int(context.getInputValue())
      context.emit(context.getInputKey(), str(s))
      context.incrementCounter(self.output_words, 1)

Counter values and status updates show up in Hadoop's web interface.
In addition, the final values of all counters are listed in the
command line output of the job (note that the list also includes Hadoop's
default counters).

Record Readers and Writers
--------------------------

By default, Hadoop assumes you want to process plain text and splits
input data into text lines.  If you need to process binary data, or
your text data is structured into records that span multiple lines,
you need to write your own :class:`~pydoop.pipes.RecordReader`.

The record reader operates at the HDFS file level: its job is to read
data from the file and feed it as a stream of key-value pairs
(records) to the Mapper.  The following examples shows how to write a
record reader that mimics Hadoop's default ``LineRecordReader``, where
keys are byte offsets with respect to the whole file and values are
text lines:

.. code-block:: python

  import struct
  import pydoop.pipes as pp
  import pydoop.hdfs as hdfs

  class Reader(pp.RecordReader):

    def __init__(self, context):
      super(Reader, self).__init__()
      self.isplit = pp.InputSplit(context.getInputSplit())
      self.file = hdfs.open(self.isplit.filename)
      self.file.seek(self.isplit.offset)
      self.bytes_read = 0
      if self.isplit.offset > 0:
        discarded = self.file.readline()  # read by reader of previous split
        self.bytes_read += len(discarded)

    def close(self):
      self.file.close()
      self.file.fs.close()

    def next(self):
      if self.bytes_read > self.isplit.length:  # end of input split
        return (False, "", "")
      key = struct.pack(">q", self.isplit.offset+self.bytes_read)
      record = self.file.readline()
      if record == "":  # end of file
        return (False, "", "")
      self.bytes_read += len(record)
      return (True, key, record)

    def getProgress(self):
      return min(float(self.bytes_read)/self.isplit.length, 1.0)

From the context, the record reader gets the following information on
the byte chunk assigned to the current task, or **input split**:

* the name of the file it belongs to;
* its offset with respect to the beginning of the file;
* its length.

This allows to open the file, seek to the correct offset and read
until the end of the split is reached.  The framework gets the record
stream by means of repeated calls to the
:meth:`~pydoop.pipes.RecordReader.next` method, expecting a tuple of
three elements:

* a boolean that, if false, signals the end of the record stream;
* the input key for the mapper;
* the input value for the mapper.

The :meth:`~pydoop.pipes.RecordReader.getProgress` method is called by
the framework to get the fraction of the input split that's already
been processed.

The ``close`` method (present in all components except for
the partitioner) is called by the framework once it has finished
retrieving the records: this is the right place to perform cleanup
tasks such as closing open handles.

Note that when you want to use your own record reader, you need to
pass the class object to the factory:

.. code-block:: python

  if __name__ == "__main__":
    pp.runTask(pp.Factory(Mapper, Reducer, record_reader_class=Reader))

Finally, when running the program, the hadoop pipes call must set the
``hadoop.pipes.java.recordreader`` option to ``false``.

The record writer writes key/value pairs to output files.  The default
behavior is to write one tab-separated key/value pair per line; if you
want to do something different, you have to write a custom
:class:`~pydoop.pipes.RecordWriter`:

.. code-block:: python

  import pydoop.pipes as pp
  import pydoop.hdfs as hdfs
  from pydoop.utils import jc_configure, jc_configure_int

  class Writer(pp.RecordWriter):

    def __init__(self, context):
      super(Writer, self).__init__(context)
      jc = context.getJobConf()
      jc_configure_int(self, jc, "mapred.task.partition", "part")
      jc_configure(self, jc, "mapred.work.output.dir", "outdir")
      jc_configure(self, jc, "mapred.textoutputformat.separator", "sep", "\t")
      jc_configure(self, jc, "pydoop.hdfs.user", "hdfs_user", None)
      self.outfn = "%s/part-%05d" % (self.outdir, self.part)
      self.file = hdfs.open(self.outfn, "w", user=self.hdfs_user)

    def close(self):
      self.file.close()
      self.file.fs.close()

    def emit(self, key, value):
      self.file.write("%s%s%s\n" % (key, self.sep, value))

The above example, which simply reproduces the default behavior, also
shows how to get job configuration parameters: the ones starting with
"mapred" are standard Hadoop parameters, while "pydoop.hdfs.user" is a
custom parameter defined by the application developer.  To set the
key-value separator and the hdfs user, for instance, the application
could be run as::

  hadoop pipes \
    -D hadoop.pipes.java.recordwriter=false \
    -D mapred.textoutputformat.separator=@ \
    -D pydoop.hdfs.user=myuser \
    [...]

Note that we had to set ``hadoop.pipes.java.recordwriter`` to
``false``, as we did for the record reader in the previous section.

Since we want to use our own record reader, we have to pass the class
object to the factory:

.. code-block:: python

  if __name__ == "__main__":
    pp.runTask(pp.Factory(Mapper, Reducer, record_writer_class=Writer))

Partitioners and Combiners
--------------------------

The :class:`~pydoop.pipes.Partitioner` assigns intermediate keys to
reducers: the default is to select the reducer on the basis of a hash
function of the key.  The following example reproduces the default
behavior:

.. code-block:: python

  import sys
  import pydoop.pipes as pp

  class Partitioner(pp.Partitioner):
  
    def __init__(self, context):
      super(Partitioner, self).__init__(context)

    def partition(self, key, n_red):
      reducer_id = (hash(key) & sys.maxint) % n_red
      return reducer_id

The framework calls the :meth:`~pydoop.pipes.Partitioner.partition`
method passing it the total number of reducers ``n_red``, and expects
the chosen reducer ID --- in the ``[0, ..., n_red-1]`` range --- as
the return value.

The combiner is functionally identical to a reducer, but it is run
locally, on the key-value stream output by a single mapper.  Although
nothing prevents the combiner from processing values differently from
the reducer, the former, provided that the reduce function is
associative and idempotent, is typically configured to be the same as
the latter, in order to perform local aggregation and thus help cut
down network traffic.

The following snippet shows how to set the partitioner and combiner
(here we use the reducer as the combiner) classes:

.. code-block:: python

  if __name__ == "__main__":
    pp.runTask(pp.Factory(
      Mapper, Reducer,
      partitioner_class=Partitioner,
      combiner_class=Reducer,
      ))
