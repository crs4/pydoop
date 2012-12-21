Introduction
============

Pydoop includes several usage examples: you can find them in the
"examples" subdirectory of the distribution root. 


Home Directory
--------------

If you've installed Pydoop or other modules locally, i.e., into
``~/.local/lib/python2.7/site-packages``, the Python code that runs
within Hadoop tasks might not be able to find them. This is due to the
fact that, according to your Hadoop version or configuration, those
tasks might run as a different user.  In Hadoop 1.0, you can work
around this problem by setting the ``mapreduce.admin.user.home.dir``
configuration parameter.  In order to make Hadoop examples work
out-of-the-box under (hopefully) any configuration, we added an
automatic hack of ``sys.path``.

**NOTE**: In any event, to allow another user to execute your locally
installed code, you must set permissions accordingly, e.g.::

    chmod -R 755 ~/.local


Input Data
----------

Most examples, by default, take their input from a free version of
Lewis Carrol's "Alice's Adventures in Wonderland" available at
`Project Gutenberg <http://www.gutenberg.org>`_ (see the
``examples/input`` sub-directory).
