.. _running_apps:

Pydoop Submit User Guide
========================

Pydoop applications are run via the ``pydoop submit`` command.  To
start, you will need a working Hadoop cluster.  If you don't have one
available, you can bring up a single-node Hadoop cluster on your
machine -- see `the Hadoop web site <http://hadoop.apache.org>`_ for
instructions. Alternatively, the source directory contains a
Dockerfile that can be used to build an image with Hadoop and Pydoop
installed and (minimally) configured. Check out ``.travis.yml`` for
usage hints.

If your application is contained in a single (local) file named
``wc.py``, with an entry point called ``__main__`` (see
:ref:`api_tutorial`) you can run it as follows::

  pydoop submit --upload-file-to-cache wc.py wc input output

where ``input`` (file or directory) and ``output`` (directory) are
HDFS paths.  Note that the ``output`` directory will not be
overwritten: instead, an error will be generated if it already exists
when you launch the program.

If your entry point has a different name, specify it via ``--entry-point``.

The following table shows command line options for ``pydoop submit``:

.. include:: pydoop_submit_options.rst


Setting the Environment for your Program
----------------------------------------

When working on a shared cluster where you don't have root access, you
might have a lot of software installed in non-standard locations, such
as your home directory. Since non-interactive ssh connections do not
usually preserve your environment, you might lose some essential
setting like ``LD_LIBRARY_PATH``\ .

For this reason, by default ``pydoop submit`` copies some environment
variables to the driver script that launches the job on Hadoop.  If
this behavior is not desired, you can disable it via the
``--no-override-env`` command line option.
