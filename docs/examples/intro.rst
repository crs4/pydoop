Introduction
============

Pydoop includes several usage examples: you can find them in the
"examples" subdirectory of the distribution root. To run them, you
need a working Hadoop cluster. If you don't have one available, you
can bring up a single-node Hadoop cluster on your machine following
the `Hadoop quickstart guide
<http://hadoop.apache.org/common/docs/r0.20.2/quickstart.html>`_\
. Configure Hadoop for "Pseudo-Distributed Operation" and start the
daemons as explained in the guide.

Pydoop applications are run as any other Hadoop Pipes applications
(e.g., C++ ones)::

  ${HADOOP_HOME}/bin/hadoop pipes -conf conf.xml -input input -output output

Where ``input`` and ``output`` are, respectively, the HDFS directory
where the applications will read its input and write its output. The
configuration file, read from the local file system, is an xml
document consisting of a simple name = value property list:

.. code-block:: xml

  <?xml version="1.0"?>
  <configuration>
  
  <property>
    <name>hadoop.pipes.executable</name>
    <value>app_executable</value>
  </property>
  
  <property>
    <name>mapred.job.name</name>
    <value>app_name</value>
  </property>
  
  <property>
    <name>hadoop.pipes.java.recordreader</name>
    <value>true</value>
  </property>
  
  <property>
    <name>hadoop.pipes.java.recordwriter</name>
    <value>true</value>
  </property>
  
  [...]

  </configuration>

``hadoop.pipes.executable`` is the HDFS path (either absolute or
relative to your HDFS home directory) of the application launcher
(i.e., the one that contains the :func:`~pydoop.pipes.runTask`
invocation); ``mapred.job.name`` is just an identifier for your
application: it will appear in the MapReduce web interface and it will
be appended to the job log file name. The remaining two properties
must be set to ``false`` if you are using your own customized
RecordReader / RecordWriter.

In the job configuration file you can set either general Hadoop
properties (e.g., ``mapred.map.tasks``\ ) or application-specific
properties: the latter are configuration parameters defined by the
application developer, whose value will be retrieved at run time
through the :class:`~pydoop.pipes.JobConf` object.

To summarize, before running your application, you need to perform the
following steps:

 * upload the application launcher and the input file or directory to HDFS
 * prepare the xml configuration file, indicating the HDFS path to the
   launcher as shown above; alternatively, you can specify the
   launcher's path it by passing it as an argument to the ``-program``
   command line option
 * check that the ``output`` directory does not exists (it will not be
   overwritten: an error will be generated instead)
