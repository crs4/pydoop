Using the Hadoop SequenceFile Format
====================================

Although many MapReduce applications deal with text files, there are
many cases where processing binary data is required. Hadoop supports
this through the `SequenceFile
<http://hadoop.apache.org/common/docs/r0.20.0/api/org/apache/hadoop/io/SequenceFile.html>`_
format, which allows to read and write arbitrary key/value pairs.

It's easy to use this feature from Pydoop. To write ``SequenceFile``\ s,
add the following properties to your job configuration file:

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
link). To read ``SequenceFile``\ s, add the following one:

.. code-block:: xml

  <property>
    <name>mapred.input.format.class</name>
    <value>org.apache.hadoop.mapred.SequenceFileInputFormat</value>
  </property>


Example Application: Filter Wordcount Results
---------------------------------------------

``SequenceFile`` is mostly useful to handle complex objects like
C-style structs or images. To keep our example as simple as possible,
we considered a situation where you wish to emit the raw bytes of an
integer value.

We wrote a trivial application that reads input from a previous
:doc:`wordcount` run and filters out words whose count falls below a
configurable threshold. Suppose you know in advance that most counts
will be large, but not so large that they cannot fit in a 32-bit
integer: since the decimal representation could require as much as 10
bytes, you decide to save space by having the Wordcount reducer emit
the raw four bytes of the integer instead:

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
