Troubleshooting
===============

#. Missing libhdfs: the current (0.20.2) Hadoop version does not
   include a pre-compiled version of libhdfs.so for 64-bit
   machines. To compile and install your own, do the following::

    cd ${HADOOP_HOME}
    chmod +x src/c++/{libhdfs,pipes,utils}/configure
    ant compile -Dcompile.c++=true -Dlibhdfs=true
    mv build/c++/Linux-amd64-64/lib/libhdfs.* c++/Linux-amd64-64/lib/
    cd c++/Linux-amd64-64/lib/
    ln -fs libhdfs.so.0.0.0 libhdfs.so

   Note that if you run a 32-bit jvm on a 64-bit platform, you need
   the 32-bit libhdfs (see `HADOOP-3344
   <https://issues.apache.org/jira/browse/HADOOP-3344>`_\ ).  In this
   case, copy the pre-compiled ``libhdfs.*`` from ``c++/Linux-i386-32/lib`` to
   ``c++/Linux-amd64-64/lib``.

#. Non-standard include/lib directories: the setup script looks for
   includes and libraries in standard places -- read setup.py for
   details. If some of the requirements are stored in different
   locations, you need to add them to the search path. Example::

    python setup.py build_ext -L/my/lib/path -I/my/include/path -R/my/lib/path
    python setup.py build_py
    python setup.py install --skip-build

