WordCount
=========

WordCount can be considered as the "hello world" of MapReduce. A
simple application that counts the occurrence of each word in a set of
text files, it is included in both the original MapReduce paper [#]_
and in the Hadoop documentation as a MapReduce programming tutorial.

Source code for the WordCount examples is located under
``examples/wordcount`` in the Pydoop distribution.


Minimal WordCount
-----------------

This example includes only the bare minimum required to run the
application. The entire application consists of just 14 lines of code::

  from pydoop.pipes import Mapper, Reducer, Factory, runTask
  
  class WordCountMapper(Mapper):
    def map(self, context):
      words = context.getInputValue().split()
      for w in words:
        context.emit(w, "1")
  
  class WordCountReducer(Reducer):
    def reduce(self, context):
      s = 0
      while context.nextValue():
        s += int(context.getInputValue())
      context.emit(context.getInputKey(), str(s))
  
  if __name__ == "__main__":
    runTask(Factory(WordCountMapper, WordCountReducer))


Full WordCount
--------------

This is a more verbose version of the above example, written with the
purpose of demonstrating most of Pydoop's MapReduce and HDFS
features. Specifically it shows how to:

 * write MapReduce components other than the required Mapper and Reducer  
 * use custom counters and send status reports to the framework
 * get job configuration parameters
 * interact with HDFS
 
The RecordReader, RecordWriter and Partitioner classes are Python
reimplementations of their default Java counterparts, the ones the
framework uses if you don't provide your own. As such they are not
needed for the application to work: they have been included only to
provide a tutorial on writing additional MapReduce components.

For further details, take a look at the source code in the
``examples/wordcount`` subdirectory of the Pydoop distribution.


.. rubric:: Footnotes

.. [#] J. Dean and S. Ghemawat, *MapReduce: simplified data processing
       on large clusters*, in OSDI '04: 6th Symposium on Operating
       Systems Design and Implementation, 2004
