Introduction
============

Pydoop includes several usage examples: you can find them in the
"examples" subdirectory of the distribution root. 


Python Dependencies
-------------------

If you've installed Pydoop or other Python packages needed by your
application in a non-standard location (e.g.,
``/opt/lib/python3.6/site-packages``), the Python code that runs within
Hadoop tasks might not be able to find them. Note that, according to your
Hadoop version or configuration, map and reduce tasks might run as a
different user than the one who launched the job. If you can't install
globally, Pydoop offers the option of shipping packages automatically
upon job submission, see the section on :ref:`installation-free
usage<self_contained>`.


Input Data
----------

Most examples, by default, take their input from a free version of
Lewis Carrol's "Alice's Adventures in Wonderland" available at
`Project Gutenberg <http://www.gutenberg.org>`_ (see the
``examples/input`` sub-directory).
