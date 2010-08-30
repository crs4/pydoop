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
   the full programming API, it is limited by the fact that cPython
   modules (including part of the standard library) cannot be used.

As a consequence, few Python programmers use Streaming or Jython
directly. A popular alternative is offered by `Dumbo
<http://klbostee.github.com/dumbo>`_, a programming framework built as
a wrapper around Streaming that allows for a more natural API-like
coding style and also includes facilities to make job running
easier. Its author, `Klaas Bosteels
<http://users.ugent.be/~klbostee/>`_, is also the author of the patch
that lifted the aforementioned UTF-8 restriction from Streaming.

This chapter is meant both as a guide for Dumbo programmers wishing to
port their code to Pydoop and as a comparison between the two for
Python programmers in search of the solution that best fits their
MapReduce programming needs.

The following sections refer to Dumbo version 0.21.


Counting IPs from an Apache Access Log
--------------------------------------

The `examples` directory includes a Pydoop reimplementation of
`Dumbo's tutorial example
<http://wiki.github.com/klbostee/dumbo/short-tutorial>`_, an
application for generating a list of the IPs that occur more
frequently in an `Apache access log
<http://httpd.apache.org/docs/1.3/logs.html#common>`_. Complete
documentation for the tutorial is included in K. Bosteels, `Fuzzy
techniques in the usage and construction of comparison measures for
music objects <http://users.ugent.be/~klbostee/thesis.pdf>`_, PhD
thesis, Ghent University, 2009).

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
    pp.runTask(pp.Factory(Mapper, Reducer, combiner_class=Reducer))


Currenty Pydoop does not provide a high-level wrapper to run jobs
(although we plan to include one in a future release). To run the
application, therefore, we could execute the following commands (the
first one is needed if the log file is not already on hdfs)::

  $ hadoop fs -put access.log access.log
  $ hadoop fs -put ipcount.py ipcount.py
  $ hadoop pipes -D hadoop.pipes.java.recordreader=true \
      -D hadoop.pipes.java.recordwriter=true \
      -program ipcount.py -input access.log -output output
  $ hadoop fs -cat output/part* | sort -k2,2nr | head -n 5

However, it's easy to wrap all steps needed to execute the application
in a driver Python script with a nice command line interface: an
example is given by the "ipcount" program in the `examples`
directory. In particular, by leveraging Pydoop's HDFS API,
manipulation of output files such as the one performed by the last
command and hdfs uploads can be done within Python, with no system
call required::

  def print_first_n(fs, output_path, n):
    ip_list = []
    for entry in fs.list_directory(output_path):
      fn = entry["name"]
      if fn.rsplit("/",1)[-1].startswith("part"):
        f = fs.open_file(fn)
        for line in f:
          ip, count = line.strip().split()
          ip_list.append((ip, int(count)))
        f.close()
    ip_list.sort(key=operator.itemgetter(1), reverse=True)
    for ip, count in ip_list[:n]:
      print "%s\t%d" % (ip, count)

To run the application, execute the following from the
`examples/ipcount` directory::

  $ ./ipcount input


Input from arbitrary files
^^^^^^^^^^^^^^^^^^^^^^^^^^

The next step in the Dumbo tutorial shows how to get additional input
from arbitrary files. Specifically, the application reads a file
containing a list of IP addresses that must not be taken into account
when building the top five list::

  class Mapper:

    def __init__(self):
      file = open("excludes.txt", "r")
      self.excludes = set(line[:−1] for line in file)
      file.close()

    def __call__(self, key, value):
      ip = value.partition(" ")[0]
      if not ip in self.excludes:
        yield ip, 1

Pydoop's implementation is quite similar::

  class Mapper(pp.Mapper):
  
    def __init__(self, context):
      super(Mapper, self).__init__(context)
      f = open("excludes.txt")
      self.excludes = set([line.strip() for line in f])
      f.close()
  
    def map(self, context):
      ip = context.getInputValue().split(None,1)[0]
      if ip not in self.excludes:
        context.emit(ip, "1")

The main difference lies in the way you distribute the "exclude.txt"
file to all cluster nodes. Dumbo takes advantage of Streaming's
`-file` option which, in turn, uses `Hadoop's distributed cache
<http://hadoop.apache.org/common/docs/r0.20.2/mapred_tutorial.html#DistributedCache>`_::

  $ dumbo start ipcount.py -hadoop /usr/local/hadoop \
      -input logs/2009/10/* -output ipcounts -file excludes.txt

In the case of Pydoop, you can use the distributed cache by setting
the following configuration parameters in your xml conf file:

.. code-block:: xml

  <property>
    <name>mapred.cache.files</name>
    <value>excludes.txt#excludes.txt</value>
  </property>

  <property>
    <name>mapred.create.symlink</name>
    <value>yes</value>
  </property>

Alternatively, you can set them directly as command line options for
pipes, by adding `-D mapred.cache.files=excludes.txt#excludes.txt -D
mapred.create.symlink=yes` right after the "pipes" command. The latter
approach is the one we used in `ipcount` (check the source code for
details). Since we made the excludes file a configurable option, in
our case you have to run::

  $ ./ipcount -e excludes.txt input

