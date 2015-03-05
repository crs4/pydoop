Parquet-avro I/O
================

FIXME
-----

**These docs are obsolete.**

This example shows how to do I/O with parquet-avro data, i.e.,
`Parquet <http://parquet.incubator.apache.org>`_ files that use the
`Avro <http://avro.apache.org>`_ object model.

Move to the example directory and compile the Java code (requires `sbt
<http://www.scala-sbt.org>`_):

.. code-block:: bash

   cd examples/parquet/java
   sbt assembly

Now you can run the example with ``examples/parquet/py/run_pavro.sh``.
The script does the following:

* run a Java MapReduce job that reads comma-separated text records and
  writes them as parquet-avro data to HDFS;
* run a Pydoop job that reads the parquet-avro data via a specialized
  ``PydoopAvroParquetInputFormat`` and implements the color count
  example (see the Avro docs);
* check color count results.

The ``ExampleParquetMRReader`` application focuses on the avro record
conversion used in ``PydoopAvroParquetInputFormat``.  It reads the
parquet-avro data and writes out the corresponding raw binary records.
The ``dump_avro_data.py`` script can be used to dump the written data.
Note that the messages can be decoded only because we have out-of-band
information on the right schema that should be used.

.. code-block:: bash

   hadoop jar ./target/ParquetMR-assembly-0.1.jar \
     it.crs4.pydoop.ExampleParquetMRReader parquets output
   python dump_avro_data.py output
