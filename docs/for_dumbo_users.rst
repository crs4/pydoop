Pydoop for Dumbo Users
======================

Pydoop is not the only way to write Hadoop applications in Python. The
"built-in" solutions are either Hadoop Streaming with Python
executables or Jython. The main disadvantages with these approaches
are the following:

#. Hadoop Streaming does not provide a true API: the developer writes
   a mapper and a reducer script that communicate with the framework
   via standard input/output. The programming style is therefore
   rather awkward, especially in the case of reducers, where
   developers must manually handle key switching. More importantly,
   there is no way to write a Python RecordReader, RecordWriter or
   Partitioner. Before Hadoop 0.21, Streaming had the additional
   limitation of only being able to process UTF-8 text records

#. Jython is a Java implementation of the Python language: the
   standard C implementation, in cases where ambiguity may arise, is
   referred to as "cPython". Although this approach gives access to
   the full programming API, it is severely limited by the fact that
   cPython modules (including part of the standard library) cannot be
   used.

As a consequence, few Python programmers use Streaming or Jython
directly. A popular alternative is offered by `Dumbo
<http://klbostee.github.com/dumbo>`_, a programming framework built as
a wrapper around Streaming that allows for a more natural API-like
coding style and also includes facilities to make job running much
easier. Its author, `Klaas Bosteels
<http://users.ugent.be/~klbostee/>`_, is also the author of the patch
that lifted the aforementioned UTF-8 restriction from Streaming.

This chapter is meant both as a guide for Dumbo programmers wishing to
try their code on Pydoop and as a comparison between the two for
Python programmers in search of the solution that best fits their
MapReduce programming needs. In a nutshell, currently Dumbo offers
more in terms of high-level application handling (user-friendly
interface, easy scheduling of multiple MapReduce iterations, etc.) and
"Pythonic" API, but it has no support for components other than
Mappers and Reducers and no HDFS support.


Counting IPs from an Apache Access Log
--------------------------------------

The `examples` directory includes a Pydoop reimplementation of
`Dumbo's tutorial example
<http://wiki.github.com/klbostee/dumbo/short-tutorial>`_, that shows
how to generate a list of the IPs that occur more frequently in an
`Apache access log
<http://httpd.apache.org/docs/1.3/logs.html#common>`_. We implemented
both the basic and the extended version of the tutorial (the latter is
included in K. Bosteels, `Fuzzy techniques in the usage and
construction of comparison measures for music objects
<http://users.ugent.be/~klbostee/thesis.pdf>`_, PhD thesis, Ghent
University, 2009).


The Dumbo MapReduce code for the basic example is::

  def mapper(key,value):
    yield value.split(" ")[0], 1
    
  def reducer(key,values):
    yield key, sum(values)
    
  if __name__ == "__main__":
    import dumbo
    dumbo.run(mapper,reducer,combiner=reducer)


and it is run with::

  $ dumbo start ipcount.py -input access.log -output ipcounts
  $ dumbo cat ipcounts | sort -k2,2nr | head -n 5


The Pydoop version of the above is::

  import pydoop.pipes as pp
    
  class Mapper(pp.Mapper):
    def map(self, context):
      context.emit(context.getInputValue().split(None,1)[0], "1")
  
  class Reducer(pp.Reducer):
    def reduce(self, context):
      s = 0
      while context.nextValue():
        s += int(context.getInputValue())
      context.emit(context.getInputKey(), str(s))
    
  if __name__ == "__main__":
    pp.runTask(pp.Factory(Mapper, Reducer))


Currenty Pydoop does not provide a high-level wrapper to run jobs
(although we plan to include one in a future release). To run the
application, therefore, we execute the following commands (the first
one is needed if the log file is not already on hdfs)::

  $ hadoop fs -put access.log access.log
  $ hadoop fs -put ipcount.py ipcount.py
  $ hadoop pipes -D hadoop.pipes.java.recordreader=true \
      -D hadoop.pipes.java.recordwriter=true \
      -program ipcount.py -input access.log -output output
  $ hadoop fs -cat output/part* | sort -k2,2nr | head -n 5

Note that it's easy to wrap all steps needed to execute the
application in a driver Python script with a nice command line
interface: look for the "ipcount" file in the `examples` directory for
an example related to the full application (see the following sections
for details). In particular, by leveraging Pydoop's HDFS API,
manipulation of output files such as the one performed by the last
command can be done within Python, with no system call required.


Input from arbitrary files
^^^^^^^^^^^^^^^^^^^^^^^^^^

