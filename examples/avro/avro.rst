Avro reading and writing example
================================

Part 1: defining models and creating a datafile
-----------------------------------------------

In user.avsc we have a variation of the usual avro schema example. 

py/write_file.py  is then used to generate a file with dummy records.


Part 2: a pure python map-reduce application
--------------------------------------------

A pure python map-reduce app that will read from an avro file and create avro
output. 

See the actual map-reduce application py/color_counts.py, the simulator driver
py/run_color_counts.py  and the hadoop driver py/run_py_only_map_reduce.sh.


Part 3: a pure java map-reduce application
------------------------------------------

A pure java map-reduce app that will read from an avro file and create avro
output.

.. code-block::

   cd java
   sbt assembly
   hadoop jar ./target/AvroMR-assembly-0.1.jar \
              examples.MapReduceColorCount \
              hdfs://localhost:9000/user/zag/{users.avro,foo}


Part 4: a pure python with reader schema
----------------------------------------

Select data subsets using a reader schema. See py/color_only_counts.py, and
related driver programs.
