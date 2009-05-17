Check if ldd is loading the right jvm.

zag@manzanillo libhdfs $ echo $CLASSPATH
/opt/hadoop-0.19.1/lib/xmlenc-0.52.jar:/opt/hadoop-0.19.1/lib/slf4j-log4j12-1.4.3.jar:/opt/hadoop-0.19.1/lib/slf4j-api-1.4.3.jar:/opt/hadoop-0.19.1/lib/servlet-api.jar:/opt/hadoop-0.19.1/lib/oro-2.0.8.jar:/opt/hadoop-0.19.1/lib/log4j-1.2.15.jar:/opt/hadoop-0.19.1/lib/kfs-0.2.0.jar:/opt/hadoop-0.19.1/lib/junit-3.8.1.jar:/opt/hadoop-0.19.1/lib/jetty-5.1.4.jar:/opt/hadoop-0.19.1/lib/jets3t-0.6.1.jar:/opt/hadoop-0.19.1/lib/hsqldb-1.8.0.10.jar:/opt/hadoop-0.19.1/lib/commons-net-1.4.1.jar:/opt/hadoop-0.19.1/lib/commons-logging-api-1.0.4.jar:/opt/hadoop-0.19.1/lib/commons-logging-1.0.4.jar:/opt/hadoop-0.19.1/lib/commons-httpclient-3.0.1.jar:/opt/hadoop-0.19.1/lib/commons-codec-1.3.jar:/opt/hadoop-0.19.1/lib/commons-cli-2.0-SNAPSHOT.jar:/opt/hadoop-0.19.1/hadoop-0.19.1-core.jar:/opt/hadoop-0.19.1/hadoop-0.19.1-tools.jar:/opt/hadoop-0.19.1/conf

zag@manzanillo src $ python
Python 2.5.4 (r254:67916, Apr 25 2009, 22:11:37) 
[GCC 4.1.2 (Gentoo 4.1.2 p1.1)] on linux2
Type "help", "copyright", "credits" or "license" for more information.
>>> import pydoop_core
>>> dir(pydoop_core) 
['Factory', 'JobConf', 'MapContext', 'Mapper', 'PartitionerBasic wrapping of Partitioner class', 'RecordReaderBasic wrapping of RecordReader class', 'RecordWriterBasic wrapping of RecordWriter class', 'ReduceContext', 'Reducer', 'TaskContext', 'TaskContext_Counter', 'TestFactory', '__doc__', '__file__', '__name__', 'double_a_string', 'get_JobConf_object', 'get_MapContext_object', 'get_ReduceContext_object', 'get_TaskContext_object', 'hdfs_file', 'hdfs_fs', 'runTask', 'try_factory', 'try_factory_internal', 'try_mapper', 'try_reducer', 'wrap_JobConf_object']
>>> fs = pydoop_core.hdfs_fs("", 0)
>>> dir(fs)     
['__class__', '__delattr__', '__dict__', '__doc__', '__getattribute__', '__hash__', '__init__', '__instance_size__', '__module__', '__new__', '__reduce__', '__reduce_ex__', '__repr__', '__setattr__', '__str__', '__weakref__', 'open_file']
>>> import os
>>> f = fs.open_file('foo.txt', os.O_WRONLY, 0, 0, 0)
size of path =7
created file foo.txt
>>> f.write('dsjahfajsdhfajs')
15
>>> f.close()
Traceback (most recent call last):
  File "<stdin>", line 1, in <module>
AttributeError: 'hdfs_file' object has no attribute 'close'
>>> 
