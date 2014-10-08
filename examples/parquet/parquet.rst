Parquet Avro examples
=====================

Part 1: writing an avro-parquet file using java
-----------------------------------------------

The following is an example java mr job that will write a parquet file.

.. code-block::

   cd java_mr_app
   sbt assembly
   python create_input.py 20 foo.dat
   hdfs dfs -put foo.dat
   hadoop jar ./target/ParquetMR-assembly-0.1.jar \
              ExampleParquetMRWrite foo.dat parquets


Part 2: reading an avro-parquet file using java
-----------------------------------------------






