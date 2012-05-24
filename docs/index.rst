.. Pydoop documentation master file, created by
   sphinx-quickstart on Sun Jun 20 17:06:55 2010.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Pydoop Documentation
====================

**Version:** |release|

Welcome to Pydoop.  Pydoop is a package that provides a Python API for
`Hadoop <http://hadoop.apache.org>`_ MapReduce and HDFS.  Pydoop has
several advantages [#pydoop]_ over Hadoop's built-in solutions for
Python programming, i.e., Hadoop Streaming and Jython: being a CPython
package, it allows you to access all standard library and third party
modules, some of which may not be available for other Python
implementations -- e.g., `NumPy <http://numpy.scipy.org>`_; in
addition, Pydoop provides a Python HDFS API which, to the best of our
knowledge, is not available in other solutions.


Easy Hadoop Scripting
+++++++++++++++++++++

In addition to its MapReduce and HDFS API, Pydoop also provides a
solution for easy Hadoop scripting which allows you to work in a way
similar to `Dumbo <https://github.com/klbostee/dumbo>`_.  This
mechanism lowers the programming effort to the point that you may
start finding yourself writing simple 3-line throw-away Hadoop
scripts!

For simple tasks such as word counting, for instance, your code would
look like this:

.. code-block:: python

  def mapper(k, text, writer):
    for word in text.split():
      writer.emit(word, 1)

  def reducer(word, count, writer):
    writer.emit(word, sum(map(int, count)))

See the :ref:`pydoop_script` page for details.


Full-fledged Hadoop API
+++++++++++++++++++++++

For more complex applications, you can use the :ref:`full API
<api-docs>`.  Here is how how a word count would look:

.. code-block:: python

  from pydoop.pipes import Mapper, Reducer, Factory, runTask

  class WordCountMapper(Mapper):

    def __init__(self, context):
      super(Mapper, self).__init__(context)
      context.setStatus("initializing")
      self.input_words = context.getCounter("WORDCOUNT", "INPUT_WORDS")

    def map(self, context):
      words = context.getInputValue().split()
      for w in words:
        context.emit(w, "1")
      context.incrementCounter(self.input_words, len(words))

  class WordCountReducer(Reducer):

    def reduce(self, context):
      s = 0
      while context.nextValue():
        s += int(context.getInputValue())
      context.emit(context.getInputKey(), str(s))

  runTask(Factory(WordCountMapper, WordCountReducer))


High-level HDFS API
+++++++++++++++++++

Pydoop includes a high-level :ref:`HDFS API <hdfs-api>` that
simplifies common tasks such as copying files and directories,
navigating through the file system, etc.  Here is a brief snippet that
shows some of these functionalities:

.. code-block:: python

  >>> import pydoop.hdfs as hdfs
  >>> hdfs.mkdir("test")
  >>> hdfs.dump("hello", "test/hello.txt")
  >>> hdfs.cp("test", "test.copy")

See the :ref:`tutorial <hdfs-api-examples>` for more examples.


How to Cite
+++++++++++

Pydoop is developed and maintained by researchers at `CRS4
<http://www.crs4.it>`_ -- Distributed Computing group.  If you use
Pydoop as part of your research work, please cite `the HPDC 2010 paper
<http://dx.doi.org/10.1145/1851476.1851594>`_.


Contents
========

.. toctree::
   :maxdepth: 2

   news
   installation
   pydoop_script
   running_pydoop_applications
   api_docs/index
   examples/index
   self_contained
   for_dumbo_users


Indices and Tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`


.. rubric:: Footnotes

.. [#pydoop] Simone Leo, Gianluigi Zanetti. `Pydoop: a Python
   MapReduce and HDFS API for Hadoop.
   <http://dx.doi.org/10.1145/1851476.1851594>`_, Proceedings Of The
   19th ACM International Symposium On High Performance Distributed
   Computing, page 819--825, 2010
