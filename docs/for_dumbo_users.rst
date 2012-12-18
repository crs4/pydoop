Pydoop for Dumbo Users
======================

Pydoop is not the only way to write Hadoop applications in Python. The
"built-in" solutions are Hadoop Streaming with Python executables and
Jython. The main disadvantages with these approaches are the
following:

#. Streaming does not provide an API: the developer writes a mapper
   and a reducer script that communicate with the framework via
   standard input/output. The programming style is therefore rather
   awkward, especially in the case of reducers, where developers must
   manually handle key switching. More importantly, there is no way to
   write a Python RecordReader, RecordWriter or Partitioner. In Hadoop
   versions that do not include the `HADOOP-1722
   <https://issues.apache.org/jira/browse/HADOOP-1722>`_ patch,
   Streaming has the additional limitation of only being able to
   process UTF-8 text records;

#. Jython is a Java implementation of the Python language: the
   standard C implementation, in cases where ambiguity may arise, is
   referred to as "CPython". Although this approach gives access to
   the full Hadoop API, it is limited by the fact that CPython
   modules (including part of the standard library) cannot be used.

As a consequence, few Python programmers use Streaming or Jython
directly. A popular alternative is offered by `Dumbo
<http://klbostee.github.com/dumbo>`_, a programming framework built as
a wrapper around Streaming that allows for a more elegant coding style
and also includes facilities to make job running easier. Its author,
Klaas Bosteels, is also the author of the aforementioned patch for
Streaming.

However, writing Python components other than the mapper, reducer and
combiner is not possible in Dumbo, and there is no HDFS API. Pydoop,
on the other hand, gives you almost complete access to MapReduce
components (you can write a Python RecordReader, RecordWriter and
Partitioner) and to HDFS without adding much complexity. As an
example, in this section we will show how to rewrite Dumbo's tutorial
example in Pydoop. Throughout the rest of this section we refer to
examples and results from Dumbo version 0.21.28.


Counting IPs from an Apache Access Log
--------------------------------------

The ``examples`` directory includes a Pydoop reimplementation of
`Dumbo's tutorial example
<http://wiki.github.com/klbostee/dumbo/short-tutorial>`_, an
application for generating a list of the IPs that occur more
frequently in an `Apache access log
<http://httpd.apache.org/docs/1.3/logs.html#common>`_.

The Dumbo MapReduce code for the basic example is:

.. code-block:: python

  def mapper(key, value):
    yield value.split(" ")[0], 1
    
  def reducer(key, values):
    yield key, sum(values)
    
  if __name__ == "__main__":
    import dumbo
    dumbo.run(mapper, reducer, combiner=reducer)

and it is run with::

  dumbo start ipcount.py -input access.log -output ipcounts
  dumbo cat ipcounts | sort -k2,2nr | head -n 5

With Pydoop, we could implement the above program using
:ref:`pydoop_script_tutorial`.  Write an ``ipcount_script.py`` module:

.. code-block:: python

  def mapper(key, value, writer):
    writer.emit(value.split(" ")[0], 1)

  def reducer(key, itervalues, writer):
    writer.emit(key, sum(map(int, itervalues)))

Then, run it with::

  pydoop script ipcount_script.py access.log ipcounts
  hadoop fs -cat output/part* | sort -k2,2nr | head -n 5

It should be noted that Pydoop Script doesn't allow you to set a combiner
class, nor to tackle more sophisticated problems, perhaps where you would need
to track a state within your mapper or reducer objects.  If you need that sort
of functionality then step up to the full Pydoop framework.  In that case, you
would implement the program above as follows:

.. code-block:: python

  #!/usr/bin/env python
  
  import pydoop.pipes as pp
  
  class Mapper(pp.Mapper):
    def map(self, context):
      context.emit(context.getInputValue().split(" ")[0], "1")
  
  class Reducer(pp.Reducer):
    def reduce(self, context):
      s = 0
      while context.nextValue():
        s += int(context.getInputValue())
      context.emit(context.getInputKey(), str(s))
  
  if __name__ == "__main__":
    pp.runTask(pp.Factory(Mapper, Reducer, combiner_class=Reducer))

To run the application, save the above code to ``ipcount_pydoop.py`` and run::

  hadoop fs -put ipcount_pydoop.py ipcount_pydoop.py
  hadoop pipes \
    -D hadoop.pipes.java.recordreader=true \
    -D hadoop.pipes.java.recordwriter=true \
    -program ipcount_pydoop.py \
    -input access.log \
    -output ipcounts
  hadoop fs -cat output/part* | sort -k2,2nr | head -n 5

It's easy enough to wrap all steps needed to execute the application
in a driver Python script with a nice command line interface: an
example is given in the ``examples/ipcount``
directory. In particular, by leveraging Pydoop's HDFS API,
manipulation of output files such as the one performed by the last
command and HDFS uploads can be done within Python, without any need
to call the Hadoop command line programs:

.. code-block:: python

  def collect_output(mr_out_dir):
    ip_list = []
    for fn in hdfs.ls(mr_out_dir):
      if hdfs.path.basename(fn).startswith("part"):
        with hdfs.open(fn) as f:
          for line in f:
            ip, count = line.strip().split("\t")
            ip_list.append((ip, int(count)))
    return ip_list

  ip_list = collect_output("ipcounts")
  ip_list.sort(key=operator.itemgetter(1), reverse=True)
  for ip, count in ip_list[:5]:
    print "%s\t%d" % (ip, count)

To run the example, do the following (from Pydoop's distribution root)::

  cd examples/ipcount
  ./run


Input from Arbitrary Files
^^^^^^^^^^^^^^^^^^^^^^^^^^

The next step in the Dumbo tutorial shows how to get additional input
from arbitrary files. Specifically, the application reads a file
containing a list of IP addresses that must not be taken into account
when building the top five list:

.. code-block:: python

  class Mapper:
  
    def __init__(self):
      file = open("excludes.txt", "r")
      self.excludes = set(line.strip() for line in file)
      file.close()
  
    def __call__(self, key, value):
      ip = value.split(" ")[0]
      if not ip in self.excludes:
        yield ip, 1

Pydoop's implementation is quite similar:

.. code-block:: python

  class Mapper(pp.Mapper):
  
    def __init__(self, context):
      super(Mapper, self).__init__(context)
      with open("excludes.txt") as f:
        self.excludes = set(line.strip() for line in f)
  
    def map(self, context):
      ip = context.getInputValue().split(None, 1)[0]
      if ip not in self.excludes:
        context.emit(ip, "1")

The main difference lies in the way you distribute the "exclude.txt"
file to all cluster nodes. Dumbo takes advantage of Streaming's
``-file`` option which, in turn, uses `Hadoop's distributed cache
<http://hadoop.apache.org/common/docs/r0.20.2/mapred_tutorial.html#DistributedCache>`_::

  $ dumbo start ipcount.py -input access.log -output ipcounts -file excludes.txt

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
latter approach is the one we used in the example (check the code for
details)::

  cd examples/ipcount
  python ipcount.py -e excludes.txt -i access.log -n 5

The ``-e`` option is turned into a MapReduce JobConf parameter by the
Python code. In the next section we will see how JobConf parameters
are passed to the MapReduce application in both Dumbo and Pydoop.


Status Reports, Counters and Configuration Parameters
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Being built as a wrapper around Streaming, Dumbo sends status reports
and counter updates to the framework via standard error. This is,
however, hidden from the programmer:

.. code-block:: python

  class Mapper:
    
    def __init__(self):
      self.status = "Initialization started"
      self.excludes_fn = self.params["excludes"]
      file = open(self.excludes_fn, "r")
      self.excludes = set(line.strip() for line in file)
      file.close()
      self.status = "Initialization done"
  
    def __call__(self, key, value):
      ip = value.split(" ")[0]
      if not ip in self.excludes:
        yield ip, 1
      else:
        self.counters["Excluded lines"] += 1

In Dumbo, values for parameters are supplied via the ``-param``
option: in this case, for instance, you would add ``-param
excludes=excludes.txt`` to Dumbo's command line.

The Pydoop equivalent of the above is:

.. code-block:: python

  class Mapper(pp.Mapper):
  
    def __init__(self, context):
      super(Mapper, self).__init__(context)
      context.setStatus("Initialization started")
      self.excluded_counter = context.getCounter("IPCOUNT", "EXCLUDED_LINES")
      jc = context.getJobConf()
      pu.jc_configure(self, jc, "ipcount.excludes", "excludes_fn", "")
      if self.excludes_fn:
        with open(self.excludes_fn) as f:
          self.excludes = set(line.strip() for line in f)
      else:
        self.excludes = set()
      context.setStatus("Initialization done")
  
    def map(self, context):
      ip = context.getInputValue().split(None,1)[0]
      if ip not in self.excludes:
        context.emit(ip, "1")
      else:
        context.incrementCounter(self.excluded_counter, 1)

The ``ipcount.excludes`` parameter is passed in the same way as any
other configuration parameter (see the distributed cache example in
the previous section).


Input and Output Formats
^^^^^^^^^^^^^^^^^^^^^^^^

Just like Dumbo, Pydoop has currently no support for writing Python
input and output format classes (however, unlike Dumbo, Pydoop allows
you to write record readers/writers). You can use Java input/output
formats by setting the ``mapred.input.format.class`` and the
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

We tested Pydoop (version 0.3.6) and Dumbo (version 0.21.28) with
their respective wordcount examples. The test we ran was very similar
to the one described in [#pydoop]_ (wordcount on 20 GB of random
English text -- average completion time over five iterations), but
this time we used only 48 CPUs distributed over 24 nodes and a block
size of 64 MB. In [#pydoop]_ we found out that pre-HADOOP-1722
Streaming was about 2.6 times slower than Pydoop, while in this test
Dumbo was only 1.9 times slower.


.. rubric:: Footnotes

.. [#pydoop] Simone Leo, Gianluigi Zanetti. `Pydoop: a Python
   MapReduce and HDFS API for Hadoop.
   <http://dx.doi.org/10.1145/1851476.1851594>`_, Proceedings Of The
   19th ACM International Symposium On High Performance Distributed
   Computing, page 819--825, 2010
