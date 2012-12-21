Using the Hadoop SequenceFile Format
====================================

Although many MapReduce applications deal with text files, there are
many cases where processing binary data is required. In this case, you
basically have two options:

#. write appropriate :class:`~pydoop.pipes.RecordReader` /
   :class:`~pydoop.pipes.RecordWriter` classes for the binary format
   you need to process
#. convert your data to Hadoop's standard `SequenceFile
   <http://hadoop.apache.org/common/docs/r0.20.0/api/org/apache/hadoop/io/SequenceFile.html>`_ format.

Using Sequence Files within Pydoop is easy. To write output key/value
pairs in the ``SequenceFile`` format, add the following properties to
your job configuration file:

.. code-block:: xml

  <property>
    <name>mapred.output.format.class</name>
    <value>org.apache.hadoop.mapred.SequenceFileOutputFormat</value>
  </property>

  <property>
    <name>mapred.output.compression.type</name>
    <value>desired_compression_type</value>
  </property>

Where ``desired_compression_type`` can be ``NONE``, ``RECORD`` or
``BLOCK`` (see the ``SequenceFile`` documentation at the above
link). To read key/value pairs written in the ``SequenceFile`` format,
add the following one:

.. code-block:: xml

  <property>
    <name>mapred.input.format.class</name>
    <value>org.apache.hadoop.mapred.SequenceFileInputFormat</value>
  </property>


Example Application: Filter Wordcount Results
---------------------------------------------

``SequenceFile`` is mostly useful to handle complex objects like
C-style structs or images. To keep our example as simple as possible,
we considered a situation where a MapReduce task needs to emit the raw
bytes of an integer value.

We wrote a trivial application that reads input from a previous
:ref:`word count <word_count>` run and filters out
words whose count falls below a
configurable threshold. Of course, the filter could have been directly
applied to the wordcount reducer: the job has been artificially split
into two runs to give a ``SequenceFile`` read / write example.

Suppose you know in advance that most counts will be large, but not so
large that they cannot fit in a 32-bit integer: since the decimal
representation could require as much as 10 bytes, you decide to save
space by having the wordcount reducer emit the raw four bytes of the
integer instead:

.. code-block:: python

  class WordCountReducer(Reducer):
  
    def reduce(self, context):
      s = 0
      while context.nextValue():
        s += int(context.getInputValue())
      context.emit(context.getInputKey(), struct.pack(">i", s))

Since newline characters can appear in the serialized values, you
cannot use the standard text format where each line contains a
tab-separated key-value pair. The problem is solved by using
``SequenceFileOutputFormat`` for wordcount and
``SequenceFileInputFormat`` for the filtering application.

For further details, see the source code under ``examples/sequence_file``\ .


Running the Example
-------------------

From the Pydoop root directory::

  cd examples/sequence_file
  ./run
