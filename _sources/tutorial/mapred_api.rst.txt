.. _api_tutorial:

Writing Full-Featured Applications
==================================

While :ref:`Pydoop Script <pydoop_script_tutorial>` allows to solve
many problems with minimal programming effort, some tasks require a
broader set of features. If your data is not simple text with one record
per line, for instance, you may need to write a record reader; if
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
following snippet shows how to write the mapper and reducer for
*wordcount*, an application that counts the occurrence of each word in a
text data set:

.. literalinclude:: ../../examples/pydoop_submit/mr/wordcount_minimal.py
   :language: python
   :start-after: DOCS_INCLUDE_START

The mapper is instantiated by the MapReduce framework that, for each
input record, calls the ``map`` method passing a ``context`` object to it.
The context serves as a communication interface between the framework
and the application: in the ``map`` method, it is used to get the current
key (not used in the above example) and value, and to emit (send back
to the framework) intermediate key-value pairs.  The reducer works in
a similar way, the main difference being the fact that the ``reduce``
method gets a set of values for each key.  The context has several
other functions that we will explore later.

To run the above program, save it to a ``wc.py`` file and execute::

  pydoop submit --upload-file-to-cache wc.py wc input output

Where ``input`` is the HDFS input directory.

See the section on :ref:`running Pydoop programs<running_apps>` for
more details.  Source code for the word count example is located under
``examples/pydoop_submit/mr`` in the Pydoop distribution.


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

.. literalinclude:: ../../examples/pydoop_submit/mr/wordcount_full.py
   :language: python
   :pyobject: Mapper

.. literalinclude:: ../../examples/pydoop_submit/mr/wordcount_full.py
   :language: python
   :pyobject: Reducer

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
The **record reader** operates at the HDFS file level: its job is to read
data from the file and feed it as a stream of key-value pairs
(records) to the mapper. To interact with HDFS files, we need to import the
``hdfs`` submodule:

.. code-block:: python

  import pydoop.hdfs as hdfs

The following example shows how to write a record reader that mimics
Hadoop's default ``LineRecordReader``, where keys are byte offsets
with respect to the whole file and values are text lines:

.. literalinclude:: ../../examples/pydoop_submit/mr/wordcount_full.py
   :language: python
   :pyobject: Reader

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

To use the reader, pass the class object to the factory with
``record_reader_class=Reader`` and, when running the program with
``pydoop submit``, set the ``--do-not-use-java-record-reader`` flag.

The **record writer** writes key/value pairs to output files. The default
behavior is to write one tab-separated key/value pair per line; if you
want to do something different, you have to write a custom
:class:`~pydoop.mapreduce.api.RecordWriter`:

.. literalinclude:: ../../examples/pydoop_submit/mr/wordcount_full.py
   :language: python
   :pyobject: Writer

The above example, which simply reproduces the default behavior, also
shows how to get job configuration parameters: the one starting with
``mapreduce`` is a standard Hadoop parameter, while ``pydoop.hdfs.user``
is a custom parameter defined by the application developer.
Configuration properties are passed as ``-D <key>=<value>`` (e.g.,
``-D mapreduce.output.textoutputformat.separator='|'``) to the submitter.

To use the writer, pass the class object to the factory with
``record_writer_class=Writer`` and, when running the program with
``pydoop submit``, set the ``--do-not-use-java-record-writer`` flag.


Partitioners and Combiners
--------------------------

The :class:`~pydoop.mapreduce.api.Partitioner` assigns intermediate keys to
reducers. If you do *not* explicitly set a partitioner via the factory,
partitioning will be done on the Java side. By default, Hadoop uses
`HashPartitioner
<https://hadoop.apache.org/docs/r3.0.0/api/org/apache/hadoop/mapreduce/lib/partition/HashPartitioner.html>`_,
which selects the reducer on the basis of a hash function of the key.

To write a custom partitioner in Python, subclass
:class:`~pydoop.mapreduce.api.Partitioner`, overriding the
:meth:`~pydoop.mapreduce.api.Partitioner.partition` method. The framework will
call this method with the current key and the total number of reducers ``N``
as the arguments, and expect the chosen reducer ID --- in the ``[0, ...,
N-1]`` range --- as the return value.

The following examples shows how to write a partitioner that simply mimics the
default ``HashPartitioner`` behavior:

.. literalinclude:: ../../examples/pydoop_submit/mr/wordcount_full.py
   :language: python
   :pyobject: Partitioner
   :prepend: from hashlib import md5

The combiner is functionally identical to a reducer, but it is run
locally, on the key-value stream output by a single mapper.  Although
nothing prevents the combiner from processing values differently from
the reducer, the former, provided that the reduce function is
associative and idempotent, is typically configured to be the same as
the latter, in order to perform local aggregation and thus help cut
down network traffic.

Local aggregation is implemented by caching intermediate key/value pairs in a
dictionary. Like in standard Java Hadoop, cache size is controlled by
``mapreduce.task.io.sort.mb`` and defaults to 100 MB. Pydoop uses
:func:`sys.getsizeof` to determine key/value size, which takes into account
Python object overhead. This can be quite substantial (e.g.,
``sys.getsizeof(b"foo") == 36``) and must be taken into account if fine tuning
is desired.

.. important:: Due to the caching, when using a combiner there are
  limitations on the types that can be used for intermediate keys and
  values. First of all, keys must be `hashable
  <https://docs.python.org/3/glossary.html>`_. In addition, values
  belonging to a mutable type should not change after having been
  emitted by the mapper. For instance, the following (however contrived)
  example would not work as expected:

  .. code-block:: python

    intermediate_value = {}

    class Mapper(api.Mapper):
      def map(self, ctx):
         intermediate_value.clear()
         intermediate_value[ctx.key] = ctx.value
         ctx.emit("foo", intermediate_value)

  For these reasons, it is recommended to use immutable types for both keys
  and values when the job includes a combiner.

Custom partitioner and combiner classes must be declared to the factory as
done above for record readers and writers. To recap, if we need to use all of
the above components, we need to instantiate the factory as:

.. literalinclude:: ../../examples/pydoop_submit/mr/wordcount_full.py
   :language: python
   :start-after: DOCS_INCLUDE_START
   :end-before: DOCS_INCLUDE_END


Profiling Your Application
--------------------------

Python has built-in support for application `profiling
<https://docs.python.org/3/library/profile.html>`_. Profiling a standalone
program is relatively straightforward: run it through ``cProfile``, store
stats in a file and use ``pstats`` to read and interpret them. A MapReduce
job, however, spawns multiple map and reduce tasks, so we need a way to
collect all stats. Pydoop supports this via a ``pstats_dir`` argument to
``run_task``:

.. code-block:: python

  pipes.run_task(factory, pstats_dir="pstats")

With the above call, Pydoop will run each MapReduce task with ``cProfile``,
and store resulting pstats files in the ``"pstats"`` directory on HDFS.
You can also enable profiling in the ``pydoop submit`` command line:

.. code-block:: bash

  pydoop submit --pstats-dir HDFS_DIR [...]

If the pstats directory is specified both ways, the one from ``run_task``
takes precedence.

Another way to do time measurements is via counters. The ``utils.misc`` module
provides a ``Timer`` object for this purpose:

.. code-block:: python

  from pydoop.utils.misc import Timer

  class Mapper(api.Mapper):

      def __init__(self, context):
          super(Mapper, self).__init__(context)
          self.timer = Timer(context)

      def map(self, context):
          with self.timer.time_block("tokenize"):
              words = context.value.split()
          for w in words:
              context.emit(w, 1)

With the above coding, the total time spent to execute
``context.value.split()`` (in ms) will be automatically accumulated in
a ``TIME_TOKENIZE`` counter under the ``Timer`` counter group.

Since profiling and timers can substantially slow down the Hadoop job, they
should only be used for performance debugging.
