Pydoop for Dumbo Users
======================

Pydoop is not the only way to write Hadoop applications in Python. The
"built-in" solutions are Hadoop Streaming with Python executables and
Jython. The main disadvantages with these approaches are the
following:

#. Streaming does not provide a true API: the developer writes a
   mapper and a reducer script that communicate with the framework via
   standard input/output. The programming style is therefore rather
   awkward, especially in the case of reducers, where developers must
   manually handle key switching. More importantly, there is no way to
   write a Python RecordReader, RecordWriter or Partitioner. In Hadoop
   versions prior to 0.21, Streaming has the additional limitation of
   only being able to process UTF-8 text records;

#. Jython is a Java implementation of the Python language: the
   standard C implementation, in cases where ambiguity may arise, is
   referred to as "CPython". Although this approach gives access to
   the full programming API, it is limited by the fact that CPython
   modules (including part of the standard library) cannot be used.

As a consequence, few Python programmers use Streaming or Jython
directly. A popular alternative is offered by `Dumbo
<http://klbostee.github.com/dumbo>`_, a programming framework built as
a wrapper around Streaming that allows for a more natural API-like
coding style and also includes facilities to make job running
easier. Its author, `Klaas Bosteels
<http://users.ugent.be/~klbostee/>`_, is also the author of the patch
that lifted the aforementioned UTF-8 restriction from
Streaming. However, writing Python components other than the mapper,
reducer and combiner is not possible, and there is no HDFS
API. Pydoop, on the other hand, gives you almost complete access to
MapReduce components (you can write a Python RecordReader,
RecordWriter and Partitioner) and to HDFS without adding much
complexity. As an example, in this section we will show how to rewrite
Dumbo's tutorial example in Pydoop. Throughout the rest of this
section we refer to examples and results from Dumbo version 0.21.28.


Counting IPs from an Apache Access Log
--------------------------------------

The ``examples`` directory includes a Pydoop reimplementation of
`Dumbo's tutorial example
<http://wiki.github.com/klbostee/dumbo/short-tutorial>`_, an
application for generating a list of the IPs that occur more
frequently in an `Apache access log
<http://httpd.apache.org/docs/1.3/logs.html#common>`_. Complete
documentation for the tutorial is included in [#f1]_.

The Dumbo MapReduce code for the basic example is::

  def mapper(key,value):
    yield value.split(" ")[0], 1
    
  def reducer(key,values):
    yield key, sum(values)
    
  if __name__ == "__main__":
    import dumbo
    dumbo.run(mapper, reducer, combiner=reducer)


and it is run with::

  $ dumbo start ipcount.py -hadoop /usr/local/hadoop \
      -input access.log -output ipcounts
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


Currently Pydoop does not provide a high-level wrapper to run jobs
(although we plan to include one in a future release). Applications
are run through the ``hadoop pipes`` command::

  $ hadoop fs -put ipcount.py ipcount.py
  $ hadoop pipes -D hadoop.pipes.java.recordreader=true \
      -D hadoop.pipes.java.recordwriter=true \
      -program ipcount.py -input access.log -output output
  $ hadoop fs -cat output/part* | sort -k2,2nr | head -n 5

However, it's easy to wrap all steps needed to execute the application
in a driver Python script with a nice command line interface: an
example is given by the "ipcount" program in the ``examples/ipcount``
directory. In particular, by leveraging Pydoop's HDFS API,
manipulation of output files such as the one performed by the last
command and HDFS uploads can be done within Python, without any need
to perform system calls::

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

To run the application via the ``ipcount`` wrapper, execute the
following from the ``examples/ipcount`` directory::

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
      self.excludes = set(line[:-1] for line in file)
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
``-file`` option which, in turn, uses `Hadoop's distributed cache
<http://hadoop.apache.org/common/docs/r0.20.2/mapred_tutorial.html#DistributedCache>`_::

  $ dumbo start ipcount.py -hadoop /usr/local/hadoop \
      -input access.log -output ipcounts -file excludes.txt

In the case of Pydoop, you can use the distributed cache by setting
the following configuration parameters in your XML conf file:

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
pipes, by adding ``-D mapred.cache.files=excludes.txt#excludes.txt -D
mapred.create.symlink=yes`` right after the ``pipes`` command. The
latter approach is the one we used in ipcount (check the source code
for details). Since we made the name of the excludes file a command
line option, in our case you would run::

  $ ./ipcount -e excludes.txt input

The "-e" option is turned into a MapReduce JobConf parameter by
``ipcount``. In the next section we will see how JobConf parameters
are passed to the MapReduce application in both Dumbo and Pydoop.


Status Reports, Counters and Configuration Parameters
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Being built as a wrapper around Streaming, Dumbo sends status reports
and counter updates to the framework via standard error. This is,
however, hidden from the programmer::

  class Mapper:
    
    def __init__(self):
      self.status = "Initialization started"
      self.excludes_fn = self.params["excludes"]
      file = open(self.excludes_fn, "r")
      self.excludes = set(line[:-1] for line in file)
      file.close()
      self.status = "Initialization done"
  
    def __call__(self, key, value):
      ip = value.partition(" ")[0]
      if not ip in self.excludes:
        yield ip, 1
      else:
        self.counters["Excluded lines"] += 1

Note that, in the above snippet, the hardwired reference to
"excludes.txt" has been replaced by a configuration parameter (this is
a modification we applied to the original tutorial, which uses a
different example). In Dumbo, values for parameters are supplied via
the ``-param`` option: in this case, for instance, you would add
``-param excludes=excludes.txt`` to Dumbo's command line.

The Pydoop equivalent of the above is::

  class Mapper(pp.Mapper):
  
    def __init__(self, context):
      super(Mapper, self).__init__(context)
      context.setStatus("Initialization started")
      self.excluded_counter = context.getCounter("IPCOUNT", "EXCLUDED_LINES")
      jc = context.getJobConf()
      pu.jc_configure(self, jc, "ipcount.excludes", "excludes_fn", "")
      if self.excludes_fn:
        f = open(self.excludes_fn)
        self.excludes = set([line.strip() for line in f])
        f.close()
      else:
        self.excludes = set([])
      context.setStatus("Initialization done")
  
    def map(self, context):
      ip = context.getInputValue().split(None,1)[0]
      if ip not in self.excludes:
        context.emit(ip, "1")
      else:
        context.incrementCounter(self.excluded_counter, 1)

The ``ipcount.excludes`` parameter is passed in the same way as any
other configuration parameter (see the distributed cache example in
the previous section). The dotted name convention is useful to avoid
clashing with standard Hadoop parameters.


Input and Output Formats
^^^^^^^^^^^^^^^^^^^^^^^^

Just like Dumbo, Pydoop has currently no support for writing Python
input and output format classes. You can use Java input/output formats
by setting the ``mapred.input.format.class`` and the
``mapred.output.format.class`` properties: see
:doc:`examples/sequence_file` for an example. Note that if you write
your own Java input/output format class, you need to pass the
corresponding jar file name to pipes via the ``-jar`` option.


Automatic Deployment of Python Packages
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Dumbo includes a ``-libegg`` option for automatic distribution of
`Python eggs
<http://peak.telecommunity.com/DevCenter/PythonEggs>`_. For an example
on how to distribute arbitrary Python packages, possibly including
Pydoop itself, to all cluster nodes, see :doc:`self_contained`\ .


Performance
^^^^^^^^^^^

We tested Pydoop's and Dumbo's performance with their respective
wordcount examples from Pydoop 0.3.6 and Dumbo 0.21.28. Since Pydoop
does not support Hadoop version 0.21 yet, we patched Hadoop 0.20.2 as
described in the `Building and Installing
<http://wiki.github.com/klbostee/dumbo/building-and-installing>`_
section of Dumbo's online documentation and rebuilt it. The test we
ran was very similar to the one described in [#f2]_ (wordcount on 20
GB of random English text -- average completion time over five
iterations), but this time we used only 48 CPUs distributed over 24
nodes and a block size of 64 MB. In [#f2]_ we found out that pre-0.21
Streaming was about 2.6 times slower than Pydoop, while in this test
Dumbo was only 1.9 times slower. This is likely due to the
introduction of binary data processing in Streaming.


.. rubric:: Footnotes

.. [#f1] K. Bosteels, `Fuzzy techniques in the usage and construction
         of comparison measures for music objects
         <http://users.ugent.be/~klbostee/thesis.pdf>`_, PhD thesis,
         Ghent University, 2009.
.. [#f2] Simone Leo and Gianluigi Zanetti, Pydoop: a Python MapReduce
         and HDFS API for Hadoop. In Proceedings of the `19th ACM
         International Symposium on High Performance Distributed
         Computing (HPDC 2010)
         <http://hpdc2010.eecs.northwestern.edu/>`_, pages
         819–825. ACM, 2010.
