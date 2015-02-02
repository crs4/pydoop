Avro-Parquet I/O
================


Writing an Avro-Parquet file using Java
---------------------------------------

The following runs an example Java MapReduce job that writes a
`Parquet <http://parquet.incubator.apache.org>`_ file (requires `sbt
<http://www.scala-sbt.org>`_):

.. code-block:: bash

   cd examples/parquet/java
   sbt assembly
   python create_input.py 20 foo.dat
   hdfs dfs -put foo.dat
   hadoop jar ./target/ParquetMR-assembly-0.1.jar \
     it.crs4.pydoop.ExampleParquetMRWrite foo.dat parquets


Reading an Avro-Parquet file using Java
---------------------------------------

The following runs a MapReduce program that reads Parquet data and
converts it to avro-encoded messages:

.. code-block:: bash

   hadoop jar ./target/ParquetMR-assembly-0.1.jar \
     it.crs4.pydoop.ExampleParquetMRReader parquets output

The following snippet shows how to read the avro data in Python.  Note
that the messages can be decoded only because we have out-of-band
information on the right schema that should be used.

.. code-block:: python

   from pydoop import hdfs
   from avro.io import DatumReader, BinaryDecoder
   import avro
   from cStringIO import StringIO

   fname = 'examples/avro/schemas/user.avsc'
   schema = avro.schema.parse(open(fname).read())
   dr = DatumReader(schema)
   parts = [x for x in hdfs.ls('output') if x.find('part-m') > -1]
   for p in parts:
       with hdfs.open(p) as f:
           data = f.read()
       f = StringIO(data)
       dec = BinaryDecoder(f)
       while f.tell() < len(data):
           u = dr.read(dec)
           print u['office'], u['favorite_color']
           f.read(1) # skip the newline inserted by TextOutputFormat


Reading an Avro-Parquet file using Python
-----------------------------------------

For a full example see ``examples/parquet/py/run_pavro.sh``.
