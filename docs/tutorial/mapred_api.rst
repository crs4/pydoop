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

.. literalinclude:: ../../examples/wordcount/bin/wordcount_minimal.py
   :language: python
   :start-after: DOCS_INCLUDE_START

The mapper is instantiated by the MapReduce framework that, for each
input record, calls the ``map`` method passing a ``context`` object to it.
The context serves as a communication interface between the framework
and the application: in the map method, it is used to get the current
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

.. literalinclude:: ../../examples/wordcount/bin/wordcount_full.py
   :language: python
   :pyobject: Mapper

.. literalinclude:: ../../examples/wordcount/bin/wordcount_full.py
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
(records) to the Mapper. To interact with HDFS files, we need to import the ``hdfs`` submodule:

.. code-block:: python

  import pydoop.hdfs as hdfs

The following example shows how to write a record reader that mimics
Hadoop's default ``LineRecordReader``, where keys are byte offsets
with respect to the whole file and values are text lines:

.. literalinclude:: ../../examples/wordcount/bin/wordcount_full.py
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

.. literalinclude:: ../../examples/wordcount/bin/wordcount_full.py
   :language: python
   :pyobject: Writer

The above example, which simply reproduces the default behavior, also
shows how to get job configuration parameters: the one starting with
``mapreduce`` is a standard Hadoop parameter, while ``pydoop.hdfs.user``
is a custom parameter defined by the application developer.
Configuration properties are passed as ``-D <key>=<value>`` (e.g.,
``-D mapred.textoutputformat.separator='|'``) to the submitter.

To use the writer, pass the class object to the factory with
``record_writer_class=Writer`` and, when running the program with
``pydoop submit``, set the ``--do-not-use-java-record-writer`` flag.


Partitioners and Combiners
--------------------------

The :class:`~pydoop.mapreduce.api.Partitioner` assigns intermediate keys to
reducers: the default is to select the reducer on the basis of a hash
function of the key:

.. literalinclude:: ../../examples/wordcount/bin/wordcount_full.py
   :language: python
   :pyobject: Partitioner
   :prepend: from hashlib import md5

The framework calls the partition method passing it the total number
of reducers ``n_reduces``, and expects the chosen reducer ID --- in
the ``[0, ..., n_reduces-1]`` range --- as the return value.

The combiner is functionally identical to a reducer, but it is run
locally, on the key-value stream output by a single mapper.  Although
nothing prevents the combiner from processing values differently from
the reducer, the former, provided that the reduce function is
associative and idempotent, is typically configured to be the same as
the latter, in order to perform local aggregation and thus help cut
down network traffic. Custom partitioner and combiner classes must be
declared to the factory as done above for record readers and writers.
To recap, if we need to use all of the above components, we need to
instantiate the factory as:

.. literalinclude:: ../../examples/wordcount/bin/wordcount_full.py
   :language: python
   :start-after: DOCS_INCLUDE_START
   :end-before: DOCS_INCLUDE_END

.. _timers:

Timers
------

``Timer`` objects can help debug performance issues in mapreduce applications:

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

Adding timers has a substantial effect on performance, so their use in
production code should be avoided. Performance debugging can also be
performed by enabling profiling::

  pydoop submit --pstats-dir HDFS_DIR [...]

If the above flag is set, each MapReduce task will be run with
`cProfile <https://docs.python.org/3/library/profile.html>`_, and
resulting pstats files will be saved to the specified HDFS directory.
