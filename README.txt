pydoop - Python MapReduce API for Hadoop


Copyright (C) Simone Leo, Gianluigi Zanetti - CRS4
Authors:
	Simone Leo <simleo@crs4.it>
	Gianluigi Zanetti <zag@crs4.it>


*** DEPENDENCIES ***

python 2.6 (http://www.python.org)
hadoop 0.20.1 (http://hadoop.apache.org)
boost >=1.35 (http://www.boost.org)


*** INSTALLATION INSTRUCTIONS ***

1. Set the JAVA_HOME and HADOOP_HOME environment variables to the
correct locations for your system. setup.py defaults respectively to
"/opt/sun-jdk" and "/opt/hadoop".

2. Run "python setup.py install" in the pydoop distribution root.

If the above does not work, please read the troubleshooting section.


*** TROUBLESHOOTING ***

1. Missing libhdfs: the current (0.20.1) Hadoop version does not
include a pre-compiled version of libhdfs.so for 64-bit machines. To
compile and install your own, do the following:

  cd ${HADOOP_HOME}
  chmod +x src/c++/{libhdfs,pipes,utils}/configure
  ant compile -Dcompile.c++=true -Dlibhdfs=true
  mv build/c++/Linux-amd64-64/lib/libhdfs.* c++/Linux-amd64-64/lib/

Note that if you run a 32-bit jvm on a 64-bit platform, you need the
32-bit libhdfs (see https://issues.apache.org/jira/browse/HADOOP-3344).
In this case, copy the pre-compiled libhdfs.* from c++/Linux-i386-32/lib
to c++/Linux-amd64-64/lib.

2. Non-standard include/lib directories: the setup script looks for
includes and libraries in standard places - read setup.py for
details. If some of the requirements are stored in different
locations, you need to add them to the search path. Example:

  python setup.py build_ext -L/my/lib/path -I/my/include/path -R/my/lib/path
  python setup.py build_py
  python setup.py install --skip-build
