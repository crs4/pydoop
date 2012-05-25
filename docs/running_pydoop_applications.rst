
Running Pydoop Applications
===========================

Pydoop applications are run as any other Hadoop Pipes applications
(e.g., `C++ ones
<http://developer.yahoo.com/hadoop/tutorial/module4.html#pipes>`_ such as 
`this wordcount example <http://wiki.apache.org/hadoop/C%2B%2BWordCount>`_).
To start, you will need a working Hadoop cluster.
If you don't have one available, you
can bring up a single-node Hadoop cluster on your machine following
the `Hadoop quickstart guide
<http://hadoop.apache.org/common/docs/r0.20.2/quickstart.html>`_.
Configure Hadoop for "Pseudo-Distributed Operation" and start the
daemons as explained in the guide.

Your pipes command line may look something like this::

  ${HADOOP_HOME}/bin/hadoop pipes -conf conf.xml -input input -output output

The paths ``input`` and ``output`` are the HDFS directories where the applications 
will read its input and write its output, respectively. The
configuration file, read from the local file system, is an xml
document consisting of a simple name = value property list explained below.

Configuration
-------------

Here's an example of a configuration file:

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

The meaning of these properties is as follows:

``hadoop.pipes.executable``:
  The HDFS path (either absolute or relative to your HDFS home directory) of 
  the application launcher (i.e., the one that contains the 
  :func:`~pydoop.pipes.runTask` invocation).
 
``mapred.job.name``:
  Just an identifier for your application. It will appear in the MapReduce web 
  interface and it will be appended to the job log file name.
 
``hadoop.pipes.java.recordreader`` and ``hadoop.pipes.java.recordwriter``:
  These must be set to ``true`` unless you are using 
  your own customized RecordReader/RecordWriter.

In the job configuration file you can also set application-specific
properties; their values will be accessible at run time through the 
:class:`~pydoop.pipes.JobConf` object.

Finally, you can include general Hadoop properties (e.g.,
``mapred.reduce.tasks``\ ).  See the Hadoop documentation for a list
of the available properties and their meanings.

.. note:: You can also configure property values on the command line
   with the ``-D property.name=value`` syntax .  You may find this
   more convenient when scripting or temporarily overriding a specific
   property value.  If you specify all required properties with the -D
   switches, the xml configuration file is not necessary.


Running
-------

Before running your application, you need to perform the
following steps:

* upload the application launcher and the input file or directory to HDFS;

* prepare the xml configuration file, indicating the HDFS path to the
  launcher as shown above; alternatively, you can specify the
  launcher's path it by passing it as an argument to the ``-program
  my_launcher`` command line option or setting the property on the
  command line with ``-D hadoop.pipes.executable=my_launcher``;

  * alternatively, set *all* properties with the -D switches as shown above

* check that the ``output`` directory does not exist (it will not be
  overwritten: an error will be generated instead).

The ``examples`` subdirectory of Pydoop's distribution root contains
several examples of Python scripts that integrate all of the above
steps into a convenient command line tool.  Documentation for the
examples is in the :ref:`examples` section.
