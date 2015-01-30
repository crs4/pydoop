.. _api_tutorial:

Writing Full-Featured Applications
==================================

While :ref:`Pydoop Script <pydoop_script_tutorial>` allows to solve
many problems with minimal programming effort, some tasks require a
broader set of features.  If your data is not structured into simple
text lines, for instance, you may need to write a record reader; if
you need to change the way intermediate keys are assigned to reducers,
you have to write your own partitioner.  These components are
accessible via the Pydoop MapReduce API.

The rest of this section serves as an introduction to MapReduce
programming with Pydoop; the :ref:`API reference <mr_api>` has
all the details.

Mappers and Reducers
--------------------

The Pydoop API is object-oriented: the application developer writes a
:class:`~pydoop.mapreduce.api.Mapper` class, whose core job is
performed by the :meth:`~pydoop.mapreduce.api.Mapper.map` method, and
a :class:`~pydoop.mapreduce.api.Reducer` class that processes data via
the :meth:`~pydoop.mapreduce.api.Reducer.reduce` method.  The
following snippet shows how to write the mapper and reducer for the
:ref:`word count <word_count>` problem:

.. code-block:: python

  import pydoop.mapreduce.api as api
  import pydoop.mapreduce.pipes as pp

  class Mapper(api.Mapper):

      def map(self, context):
          words = context.value.split()
          for w in words:
              context.emit(w, 1)

  class Reducer(api.Reducer):

      def reduce(self, context):
          s = sum(context.values)
          context.emit(context.key, s)

  def __main__():
      pp.run_task(pp.Factory(Mapper, Reducer))

The mapper is instantiated by the Hadoop framework that, for each
input record, calls the map method passing a ``context`` object to it.
The context serves as a communication interface between the framework
and the application: in the map method, it is used to get the current
key (not used in the above example) and value, and to emit (send back
to the framework) intermediate key-value pairs.  The reducer works in
a similar way, the main difference being the fact that the reduce
method gets a set of values for each key.  The context has several
other functions that we will explore later.

To run the above program, save it to a ``wc.py`` file and execute::

  pydoop submit --upload-file-to-cache wc.py wc input output

Where ``input`` is the HDFS input directory.

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

The following snippet shows how to modify the above example to use
counters and status updates:

.. code-block:: python

  class Mapper(api.Mapper):

      def __init__(self, context):
          super(Mapper, self).__init__(context)
          context.setStatus("initializing mapper")
          self.input_words = context.get_counter("WC", "INPUT_WORDS")

      def map(self, context):
          words = context.value.split()
          for w in words:
              context.emit(w, 1)
          context.increment_counter(self.input_words, len(words))

  class Reducer(api.Reducer):

      def __init__(self, context):
          super(Reducer, self).__init__(context)
          context.set_status("initializing reducer")
          self.output_words = context.get_counter("WC", "OUTPUT_WORDS")

      def reduce(self, context):
          s = sum(context.values)
          context.emit(context.key, s)
          context.increment_counter(self.output_words, 1)

Counter values and status updates show up in Hadoop's web interface.
In addition, the final values of all counters are listed in the
command line output of the job (note that the list also includes Hadoop's
default counters).

Record Readers and Writers
--------------------------

By default, Hadoop assumes you want to process plain text and splits
input data into text lines.  If you need to process binary data, or
your text data is structured into records that span multiple lines,
you need to write your own :class:`~pydoop.mapreduce.api.RecordReader`.

The record reader operates at the HDFS file level: its job is to read
data from the file and feed it as a stream of key-value pairs
(records) to the Mapper.  The following examples shows how to write a
record reader that mimics Hadoop's default ``LineRecordReader``, where
keys are byte offsets with respect to the whole file and values are
text lines:

.. code-block:: python

  from pydoop.utils.serialize import serialize_to_string
  import pydoop.hdfs as hdfs

  class Reader(api.RecordReader):

      def __init__(self, context):
          super(Reader, self).__init__(context)
          self.isplit = context.input_split
          self.file = hdfs.open(self.isplit.filename)
          self.file.seek(self.isplit.offset)
          self.bytes_read = 0
          if self.isplit.offset > 0:
	      # read by reader of previous split
              discarded = self.file.readline()
              self.bytes_read += len(discarded)

      def close(self):
          self.file.close()
          self.file.fs.close()

      def next(self):
          if self.bytes_read > self.isplit.length:  # end of input split
              raise StopIteration
          key = serialize_to_string(self.isplit.offset + self.bytes_read)
          record = self.file.readline()
          if record == "":  # end of file
              raise StopIteration
          self.bytes_read += len(record)
          return key, record

      def get_progress(self):
          return min(float(self.bytes_read)/self.isplit.length, 1.0)

Note that when you want to use your own record reader, you need to
pass the class object to the factory:

.. code-block:: python

  def __main__():
      pp.run_task(pp.Factory(Mapper, Reducer, record_reader_class=Reader))

From the context, the record reader gets the following information on
the byte chunk assigned to the current task, or **input split**:

* the name of the file it belongs to;
* its offset with respect to the beginning of the file;
* its length.

This allows to open the file, seek to the correct offset and read
until the end of the split is reached.  The framework gets the record
stream by means of repeated calls to the
:meth:`~pydoop.mapreduce.api.RecordReader.next` method.  The
:meth:`~pydoop.mapreduce.api.RecordReader.get_progress` method is
called by the framework to get the fraction of the input split that's
already been processed.  The ``close`` method (present in all
components except for the partitioner) is called by the framework once
it has finished retrieving the records: this is the right place to
perform cleanup tasks such as closing open handles.

When running the program, pass the ``--do-not-use-java-record-reader``
option to ``pydoop submit``.

The record writer writes key/value pairs to output files.  The default
behavior is to write one tab-separated key/value pair per line; if you
want to do something different, you have to write a custom
:class:`~pydoop.mapreduce.api.RecordWriter`:

.. code-block:: python

  class Writer(api.RecordWriter):

      def __init__(self, context):
          super(Writer, self).__init__(context)
          jc = context.job_conf
          part = jc.get_int("mapred.task.partition")
          out_dir = jc["mapred.work.output.dir"]
          outfn = "%s/part-%05d" % (out_dir, part)
          hdfs_user = jc.get("pydoop.hdfs.user", None)
          self.file = hdfs.open(outfn, "w", user=hdfs_user)
          self.sep = jc.get("mapred.textoutputformat.separator", "\t")

      def close(self):
          self.file.close()
          self.file.fs.close()

      def emit(self, key, value):
          self.file.write("%s%s%s\n" % (key, self.sep, value))

Since we want to use our own record reader, we have to pass the class
object to the factory:

.. code-block:: python

  def __main__():
      pp.run_task(pp.Factory(Mapper, Reducer, record_writer_class=Writer))

The above example, which simply reproduces the default behavior, also
shows how to get job configuration parameters: the ones starting with
``mapred`` are standard Hadoop parameters, while ``pydoop.hdfs.user``
is a custom parameter defined by the application developer.
Configuration properties are passed as ``-D <key>=<value>`` (e.g.,
``-D mapred.textoutputformat.separator='|'``) to the submitter; to
declare that we are using our own record writer, we also have to set
the ``--do-not-use-java-record-writer`` flag.

Partitioners and Combiners
--------------------------

The :class:`~pydoop.mapreduce.api.Partitioner` assigns intermediate keys to
reducers: the default is to select the reducer on the basis of a hash
function of the key.  The following example reproduces the default
behavior:

.. code-block:: python

  class Partitioner(api.Partitioner):

      def partition(self, key, n_red):
          reducer_id = (hash(key) & sys.maxint) % n_red
          return reducer_id

The framework calls the partition
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

  pp.runTask(pp.Factory(Mapper, Reducer, partitioner_class=Partitioner,
      combiner_class=Reducer))
