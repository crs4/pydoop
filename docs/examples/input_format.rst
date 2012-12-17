.. _input_format_example:

Writing a Custom InputFormat
============================

You can use a custom Java InputFormat together with a Python
:class:`~pydoop.pipes.RecordReader`: the RecordReader supplied by the
InputFormat will be overridden by the Python one.

Consider the following simple modification of Hadoop's built-in
``TextInputFormat``:

.. code-block:: java
  
  package net.sourceforge.pydoop.mapred;
  
  import java.io.*;
  
  import org.apache.hadoop.fs.*;
  import org.apache.hadoop.mapred.*;
  import org.apache.hadoop.io.LongWritable;
  import org.apache.hadoop.io.Text;
  
  
  public class TextInputFormat extends FileInputFormat<LongWritable, Text>
      implements JobConfigurable {
      
      private Boolean will_split;
  
      public void configure(JobConf conf) {
  	will_split = conf.getBoolean("pydoop.input.issplitable", true);
      }
  
      protected boolean isSplitable(FileSystem fs, Path file) {
  	return will_split;
      }
      
      public RecordReader<LongWritable, Text> getRecordReader(
          InputSplit genericSplit, JobConf job, Reporter reporter
          )
  	throws IOException {
  	reporter.setStatus(genericSplit.toString());
  	return new LineRecordReader(job, (FileSplit) genericSplit);
      }
  }


With respect to the default one, this InputFormat adds a configurable
boolean parameter (``pydoop.input.issplitable``) that, if set to
``false``, makes input files non-splitable (i.e., you can't get more
input splits than the number of input files).

For details on how to compile the above code into a jar and use it
with pipes, see ``examples/input_format`` in the Pydoop distribution
root.  You can run the input_format example with::

  cd examples/input_format
  ./run
