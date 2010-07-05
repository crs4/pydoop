Introduction
============

Pydoop includes several usage examples: you can find them in the
"examples" subdirectory of the distribution root. To run them, you
need a working Hadoop cluster. If you don't have one available, you
can bring up a single-node Hadoop cluster on your machine following
the `Hadoop quickstart guide
<http://hadoop.apache.org/common/docs/r0.20.1/quickstart.html>`_\
. Configure Hadoop for "Pseudo-Distributed Operation" and start the
daemons as explained in the guide.

Pydoop applications are run as any other Hadoop Pipes applications
(e.g., C++ ones)::

  ${HADOOP_HOME}/bin/hadoop pipes -conf conf.xml -input input -output output

Where ``input`` and ``output`` are, respectively, the HDFS directory
where the applications will read its input and write its output. The
configuration file, read from the local file system, is typically
structured as follows::

  <?xml version="1.0"?>
  <configuration>
  <property>
    <name>hadoop.pipes.executable</name>
    <value>app_executable</value>
  </property>
  <property>
    <name>hadoop.pipes.java.recordreader</name>
    <value>true</value>
  </property>
  <property>
    <name>hadoop.pipes.java.recordwriter</name>
    <value>true</value>
  </property>
  </configuration>

The value of the ``hadoop.pipes.executable`` property is the HDFS path
(absolute or relative to your HDFS home directory) of the application
launcher (i.e., the one that contains the ``runTask`` invocation).

To summarize, before running your application, you need to perform the
following steps:

 * upload the application launcher and the input file or directory to HDFS
 * prepare the xml configuration file, indicating the HDFS path to the
   launcher as shown above; alternatively, you can specify it by passing it
   as an argument to the ``-program`` pipes command line option
 * check that the ``output`` directory does not exists (it will not be
   overwritten; an error will be generated instead)
