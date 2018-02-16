.. _input_format_example:

Writing a Custom InputFormat
============================

You can use a custom Java ``InputFormat`` together with a Python
:class:`~pydoop.mapreduce.api.RecordReader`: the java RecordReader
supplied by the ``InputFormat`` will be overridden by the Python one.

Consider the following simple modification of Hadoop's built-in
``TextInputFormat``:

.. literalinclude:: ../../examples/input_format/it/crs4/pydoop/mapreduce/TextInputFormat.java
   :language: java
   :start-after: DOCS_INCLUDE_START

With respect to the default one, this InputFormat adds a configurable
boolean parameter (``pydoop.input.issplitable``) that, if set to
``false``, makes input files non-splitable (i.e., you can't get more
input splits than the number of input files).

For details on how to compile the above code into a jar and use it
with Pydoop, see ``examples/input_format``\ .
