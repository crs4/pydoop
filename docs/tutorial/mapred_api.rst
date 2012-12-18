Writing Full-Featured MapReduce Applications
============================================

While :ref:`Pydoop Script <pydoop_script_tutorial>` allows to solve
many problems with minimal programming effort, some tasks require a
broader set of features.  If your data is not structured into simple
text lines, for instance, you may need to write a record reader; if
you need to change the way intermediate keys are assigned to reducers,
you have to write your own partitioner.  These components are
accessible via the Pydoop MapReduce API, a Python wrapper of the `C++
one <http://wiki.apache.org/hadoop/C%2B%2BWordCount>`_.

The rest of this section serves as an introduction to MapReduce
programming with Pydoop; the :ref:`full API reference <api-docs>` has
all the details.

Word Count
----------

The Pydoop API is object-oriented: the application developer writes a
``Mapper`` class, whose core job is performed by the ``map`` method,
and a ``Reducer`` class that processes data via the ``reduce`` method:

.. code-block:: python

  import pydoop.pipes as pp

  class Mapper(pp.Mapper):

    def map(self, context):
      words = context.getInputValue().split()
      for w in words:
        context.emit(w, "1")

  class Reducer(pp.Reducer):

    def reduce(self, context):
      s = 0
      while context.nextValue():
        s += int(context.getInputValue())
      context.emit(context.getInputKey(), str(s))

  if __name__ == "__main__":
    pp.runTask(pp.Factory(Mapper, Reducer))


The ``Mapper`` class is instantiated by the Hadoop framework that, for
each input record, calls the ``map`` method passing a ``context``
object to it.  The context serves as a communication interface between
the framework and the application.  In the ``map`` method, it is used
to get the current key (not used in the above example) and value, and
to emit (send back to the framework) intermediate key-value pairs.
The reducer works in a similar way, the main difference being the fact
that the ``reduce`` method gets a set of values for each key.  The
context has several other functions that we will explore later.


.. note::

  TODO: show how to use optional components
