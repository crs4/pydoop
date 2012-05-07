.. Pydoop documentation master file, created by
   sphinx-quickstart on Sun Jun 20 17:06:55 2010.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Pydoop Documentation
====================

**Version:** |release|

Welcome to Pydoop.  Pydoop is a package that provides a Python API for
Hadoop MapReduce and HDFS.  As opposed to other solutions for Hadoop
Python programming, Pydoop uses the CPython interpreter and as such
allows you to access all the regular Python modules, some of which may
not be available for other Python interpreters---e.g., `NumPy
<numpy.scipy.org>`_.  Pydoop is based on Hadoop pipes, which is
slightly faster than Hadoop streaming.  In addition, Pydoop provides a
native Python HDFS API which, to the best of our knowledge, is not
available in other solutions.


Easy Hadoop scripting
+++++++++++++++++++++

In addition to its complete MapReduce API, Pydoop also provides a solution for
easy Hadoop scripting which allows you to work in a way similar to 
`Dumbo <https://github.com/klbostee/dumbo>`_.  This mechanism lowers the
programming effort to the point that you may start finding yourself writing 
simple 3-line throw-away Hadoop scripts!

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


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
