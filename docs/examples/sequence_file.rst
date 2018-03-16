Using the Hadoop SequenceFile Format
====================================

Although many MapReduce applications deal with text files, there are
many cases where processing binary data is required. In this case, you
basically have two options:

#. write appropriate :class:`~pydoop.mapreduce.api.RecordReader` /
   :class:`~pydoop.mapreduce.api.RecordWriter` classes for the binary format
   you need to process
#. convert your data to Hadoop's standard ``SequenceFile`` format.

To write sequence files with Pydoop, set the ouput format and the
compression type as follows::

  pydoop submit \
  --output-format=org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat \
  -D mapreduce.output.fileoutputformat.compress.type=NONE|RECORD|BLOCK [...]

To read sequence files, set the input format as follows::

  pydoop submit \
  --input-format=org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat


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

.. literalinclude:: ../../examples/sequence_file/bin/wordcount.py
   :language: python
   :pyobject: WordCountReducer

Since newline characters can appear in the serialized values, you
cannot use the standard text format where each line contains a
tab-separated key-value pair. The problem can be solved by using
``SequenceFileOutputFormat`` for wordcount and
``SequenceFileInputFormat`` for the filtering application.

The full source code for the example is available under
``examples/sequence_file``\ .
