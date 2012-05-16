Introduction
============

Pydoop includes several usage examples: you can find them in the
"examples" subdirectory of the distribution root. 


Remarks on running the examples
-------------------------------

Some tips for running the examples and other Pydoop applications.

Home directory
++++++++++++++

If you've installed Pydoop or other modules locally, i.e., into
``~/.local/lib/python2.7/site-packages``, the Python code that runs
within Hadoop tasks might not be able to find them. This is due to the
fact that, according to your Hadoop version or configuration, those
tasks might run as a different user. In Hadoop 1.0, you can work
around this problem by setting the ``mapreduce.admin.user.home.dir``
configuration parameter. In order to make Hadoop examples work under
(hopefully) any configuration, we added an automatic hack of
``sys.path``.

**NOTE**: In any event, to allow another user to execute your locally
installed code, you must set permissions accordingly, e.g.::

    chmod -R 755 ~/.local


Setting the environment for your program
+++++++++++++++++++++++++++++++++++++++++++

When working on a shared cluster where you don't have root access, you
might have a lot of software installed in non-standard locations, such
as your home directory. Since non-interactive ssh connections do not
read your ``~/.profile`` or ``~/.bashrc``\ , you might lose some
essential setting like ``LD_LIBRARY_PATH``\ .

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
script is interpreted by Python. Here is a quick Python script that
performs the above launcher adjustment (it assumes you've already set
the desired ``LD_LIBRARY_PATH`` in your ``.profile`` or ``.bashrc``\
):

.. code-block:: python

  import sys, os
  
  NEW_HEADER = """#!/bin/sh
  
  ""\":"
  export LD_LIBRARY_PATH="%s"
  exec %s -u $0 $@
  ":""\"
  """
    
  try:
    script = sys.argv[1]
  except IndexError:
    sys.exit("Usage: %s SCRIPT" % sys.argv[0])
  try:
    LD_LIBRARY_PATH = os.environ["LD_LIBRARY_PATH"]
  except KeyError:
    sys.exit("ERROR: could not get LD_LIBRARY_PATH!")
  f = open(script)
  code = f.read()
  f.close()
  of = open("%s.bak" % script, "w")
  of.write(code)
  of.close()
  if code.startswith("#!"):
    code = code.split(os.linesep, 1)[1]
  code = NEW_HEADER % (LD_LIBRARY_PATH, sys.executable) + code
  of = open(script, "w")
  of.write(code)
  of.close()
