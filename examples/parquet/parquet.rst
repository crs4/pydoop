Parquet Avro examples
=====================

Part 1: writing an avro-parquet file using java
-----------------------------------------------

The following is an example java mr job that will write a parquet file.

.. code-block:: bash

   cd java_mr_app
   sbt assembly
   python create_input.py 20 foo.dat
   hdfs dfs -put foo.dat
   hadoop jar ./target/ParquetMR-assembly-0.1.jar \
              it.crs4.pydoop.ExampleParquetMRWrite foo.dat parquets


Part 2: reading an avro-parquet file using java
-----------------------------------------------

With the following MR program we read the parquet data and convert it to avro
binary encoded messages.

.. code-block:: bash

   hadoop jar ./target/ParquetMR-assembly-0.1.jar \
              it.crs4.pydoop.ExampleParquetMRReader parquets output

The following code block will read all the avro encoded messages. Note that the
messages can be decoded only because we have out-of-band information on the
right schema that should be used.

.. code-block:: python

   from pydoop import hdfs
   from avro.io import DatumReader, BinaryDecoder
   import avro
   from cStringIO import StringIO

   fname = '../../avro/schemas/user.avsc'
   schema = avro.schema.parse(open(fname).read()) 
   dr = DatumReader(schema)
   parts = [ x for x in hdfs.ls('output') 
               if x.find('part-m') > -1]
   for p in parts:
       with hdfs.open(p) as f:
           data = f.read()
       f = StringIO(data)
       dec = BinaryDecoder(f)
       while f.tell() < len(data):
           print dr.read(dec)
           f.read(1) # skip the newline inserted by TextOutputFormat


Part 3: reading an avro-parquet file using python
-------------------------------------------------

.. code-block:: python

    


           
       
       



