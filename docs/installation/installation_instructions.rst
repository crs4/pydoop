Installation Instructions
=========================

#. Set the ``JAVA_HOME`` and ``HADOOP_HOME`` environment variables to
   the correct locations for your system. setup.py defaults
   respectively to ``/opt/sun-jdk`` and ``/opt/hadoop``.

#. Run ``python setup.py install`` in the Pydoop distribution root.

If the above does not work, please read the :doc:`troubleshooting`
section.

**Note for Ubuntu users:** Pydoop has been developed and tested on
Gentoo Linux. With the latest Ubuntu version and Hadoop 0.20.2, it
should build without problems. However, a build test with Ubuntu 9.10
64-bit and Hadoop 0.20.1 required us to apply a patch to the original
Hadoop Pipes C++ code first. The patch file is included in Pydoop's
distribution root as ``pipes_ubuntu.patch``\ .
