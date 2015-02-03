Parquet-avro I/O
================

The following runs an example Java MapReduce job that writes
parquet-avro data, i.e., `Parquet
<http://parquet.incubator.apache.org>`_ files that use the `Avro
<http://avro.apache.org>`_ object model (requires `sbt
<http://www.scala-sbt.org>`_):

.. code-block:: bash

   cd examples/parquet/java
   sbt assembly
   python create_input.py 20 foo.dat
   hdfs dfs -put foo.dat
   hdfs dfs -put ../../avro/schemas/user.avsc
   hadoop jar ./target/ParquetMR-assembly-0.1.jar \
     it.crs4.pydoop.ExampleParquetMRWrite foo.dat parquets user.avsc

The following runs a MapReduce program that reads Parquet data and
converts it to avro-encoded messages:

.. code-block:: bash

   hadoop jar ./target/ParquetMR-assembly-0.1.jar \
     it.crs4.pydoop.ExampleParquetMRReader parquets output

The following shows how to read the avro data in Python.  Note
that the messages can be decoded only because we have out-of-band
information on the right schema that should be used.

.. code-block:: bash

   python dump_avro_data.py output

For a full example see ``examples/parquet/py/run_pavro.sh``.
