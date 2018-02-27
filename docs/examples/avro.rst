.. _avro_io:

Avro I/O
========

Pydoop transparently supports reading and writing `Avro
<http://avro.apache.org>`_ records in MapReduce applications. This is
only available when applications are run via ``pydoop submit`` (rather
than via ``hadoop pipes``).

The following program implements a (slightly
modified) version of the color count example from the Avro docs:

.. literalinclude:: ../../examples/avro/py/color_count.py
   :language: python
   :start-after: DOCS_INCLUDE_START

The application counts the per-office occurrence of favorite colors in
a dataset of user records with the following structure:

.. literalinclude:: ../../examples/avro/schemas/user.avsc
   :language: javascript

User records are read from an Avro container stored on HDFS, and
results are written to another Avro container with the following
schema:

.. literalinclude:: ../../examples/avro/schemas/stats.avsc
   :language: javascript

Pydoop transparently serializes and/or deserializes Avro data as
needed, allowing you to work directly with Python dictionaries.  To
get this behavior, you have to set the context class to
``AvroContext``, as shown above.  Moreover, when launching the
application with pydoop submit, you have to enable Avro I/O and
specify the output schema as follows:

.. code-block:: bash

  export STATS_SCHEMA=$(cat stats.avsc)
  pydoop submit \
    -D pydoop.mapreduce.avro.value.output.schema="${STATS_SCHEMA}" \
    --avro-input v --avro-output v \
    --upload-file-to-cache color_count.py \
    color_count input output

The ``--avro-input v`` and ``--avro-output v`` flags specify that we
want to work with Avro records on MapReduce values; the other possible
choices are ``"k"``, where records are exchanged over keys, and
``"kv"``, which assumes that the top-level record structure has two
fields named ``"key"`` and ``"value"`` and passes the former on keys
and the latter on values.

Note that we did not have to specify any input schema: in this case,
Avro automatically falls back to the *writer schema*, i.e., the one
that's been used to write the container file.

The ``examples/avro`` directory contains examples for all I/O modes.


Avro-Parquet I/O
----------------

The above example focuses on `Avro containers
<http://avro.apache.org/docs/1.7.6/spec.html#Object+Container+Files>`_.
However, Pydoop supports any input/output format that exchanges Avro
records.  In particular, it can be used to read from and write to
Avro-Parquet files, i.e., `Parquet
<http://parquet.incubator.apache.org>`_ files that use the Avro object
model.

.. note::

  Make sure you have Parquet version 1.6 or later to avoid running
  into `object reuse problems
  <https://issues.apache.org/jira/browse/PARQUET-62>`_.  More
  generally, the record writer must be aware of the fact that records
  passed to its ``write`` method are mutable and can be reused by the
  caller.

The following application reproduces the k-mer count example from the
`ADAM <https://github.com/bigdatagenomics/adam>`_ docs:

.. literalinclude:: ../../examples/avro/py/kmer_count.py
   :language: python
   :start-after: DOCS_INCLUDE_START

To run the above program, execute pydoop submit as follows:

.. code-block:: bash

  export PROJECTION=$(cat projection.avsc)
  pydoop submit \
     -D parquet.avro.projection="${PROJECTION}" \
    --upload-file-to-cache kmer_count.py \
    --input-format parquet.avro.AvroParquetInputFormat \
    --avro-input v --libjars "path/to/the/parquet/jar" \
    kmer_count input output

Since we are using an external input format (Avro container input and
output formats are integrated into the Java Pydoop code), we have to
specify the corresponding class via ``--input-format`` and its jar
with ``--libjars``.  The optional parquet projection allows to extract
only selected fields from the input data.  Note that, in this case,
reading input records from values is not an option: that's how
``AvroParquetInputFormat`` works.

More Avro-Parquet examples are available under ``examples/avro``.


Running the examples
--------------------

To run the Avro examples you have to install the Python Avro package
(you can get it from the Avro web site), while the ``avro`` jar is
included in Hadoop and the ``avro-mapred`` one is included in
Pydoop.  Part of the examples code (e.g., input generation) is written
in Java.  To compile it, you need `sbt <http://www.scala-sbt.org>`_.

Move to the examples directory and compile the Java code:

.. code-block:: bash

   cd examples/avro/java
   sbt assembly

Now you should be able to run all examples under ``examples/avro/py``.
