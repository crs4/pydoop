Testing Your Installation
=========================

**NOTE:** in order to run all HDFS tests you need to:

#. set ``HADOOP_HOME`` (and possibly ``HADOOP_CONF_DIR``, if it does
   not point to ``${HADOOP_HOME}/conf``\) to the correct location for
   your system
#. start HDFS::

     ${HADOOP_HOME}/bin/start-dfs.sh

After pydoop has been successfully installed, you might want to run
unit tests. Move to the ``test`` subdirectory and run::

 python all_tests.py

You can also separately run ``python all_tests_pipes.py``
and ``python all_tests_hdfs.py``\ .
