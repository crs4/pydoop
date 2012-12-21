.. _running_apps:

Running Pydoop Applications
===========================

Pydoop applications are run as `Hadoop Pipes
<http://wiki.apache.org/hadoop/C%2B%2BWordCount>`_ applications.  To
start, you will need a working Hadoop cluster.  If you don't have one
available, you can bring up a single-node Hadoop cluster on your
machine -- see `the Hadoop web site <http://hadoop.apache.org>`_ for
instructions.

Assuming the ``hadoop`` executable is in your path, a typical pipes
command line looks like this::

  hadoop pipes -conf conf.xml -input input -output output

where ``input`` (file or directory) and ``output`` (directory) are
HDFS paths.  The configuration file, read from the local file system,
is an xml document consisting of a simple (name, value) property list
as explained below.

Configuration
-------------

Here's an example of a configuration file:

.. code-block:: xml

  <?xml version="1.0"?>
  <configuration>
  
  <property>
    <name>hadoop.pipes.executable</name>
    <value>app_launcher</value>
  </property>
  
  <property>
    <name>hadoop.pipes.java.recordreader</name>
    <value>true</value>
  </property>
  
  <property>
    <name>hadoop.pipes.java.recordwriter</name>
    <value>true</value>
  </property>
  
  <property>
    <name>mapred.job.name</name>
    <value>app_name</value>
  </property>

  [...]

  </configuration>

The meaning of these properties is as follows:

``hadoop.pipes.executable`` (required):
  The HDFS path (either absolute or relative to your HDFS home directory) of 
  the application launcher (i.e., the one that contains the 
  :func:`~pydoop.pipes.runTask` invocation).
 
``hadoop.pipes.java.recordreader`` (required): set to ``true`` unless
  you are using your own customized RecordReader.

``hadoop.pipes.java.recordwriter`` (required): set to ``true`` unless
  you are using your own customized RecordWriter.

``mapred.job.name`` (optional):
  Just an identifier for your application.  It will appear in the
  MapReduce web interface and it will be appended to the job log file
  name.

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
   switches, the xml configuration file is **not** necessary.

Setting the Environment for your Program
----------------------------------------

When working on a shared cluster where you don't have root access, you
might have a lot of software installed in non-standard locations, such
as your home directory. Since non-interactive ssh connections do not
usually preserve your environment, you might lose some essential
setting like ``LD_LIBRARY_PATH``\ .

A quick way to fix this is to insert a snippet like this one at the start of
your launcher program:

.. code-block:: bash

  #!/bin/sh

  """:"
  export LD_LIBRARY_PATH="my/lib/path:${LD_LIBRARY_PATH}"
  exec /path/to/pyexe/python -u $0 $@
  ":"""

  # Python code for the launcher follows

In this way, the launcher is run as a shell script that does some
exports and then executes Python on itself. Note that sh code is
protected by a Python comment, so that it's not considered when the
script is interpreted by Python.

Running
-------

Before running your application, you need to perform the
following steps:

* upload the application launcher and the input file or directory to HDFS;

* prepare the xml configuration file, indicating the HDFS path to the
  launcher as shown above;

  * alternatively, set **all** properties with the -D switches as
    shown above; note that you can specify the launcher's path as ``-D
    hadoop.pipes.executable=my_launcher`` or as ``-program
    app_launcher`` --- in any event, ``-D`` switches must come before
    any other option;

* check that the ``output`` directory does not exist (it will not be
  overwritten: an error will be generated instead).

The ``examples`` subdirectory of Pydoop's distribution root contains
several examples of Python scripts that generate hadoop pipes command
lines.  Documentation for the examples is in the :ref:`examples`
section.
