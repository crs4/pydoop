.. _self_contained:

Installation-free Usage
=======================

This example shows how to use the Hadoop Distributed Cache (DC) to
distribute Python packages, possibly including Pydoop itself, to all
cluster nodes at job launch time. This is useful in all cases where
installing to each node is not feasible (e.g., lack of a shared mount
point). Of course, Hadoop itself must be already installed and
properly configured in all cluster nodes before you can run this.

Source code for this example is available under ``examples/self_contained``\ .


Example Application: Count Vowels
---------------------------------

The example MapReduce application, ``vowelcount``, is rather trivial: it counts
the occurrence of each vowel in the input text. Since the point here
is to show how a structured package can be distributed and imported,
the implementation is exceedingly verbose.

.. literalinclude:: ../examples/self_contained/vowelcount/lib/__init__.py
   :language: python
   :start-after: DOCS_INCLUDE_START

.. literalinclude:: ../examples/self_contained/vowelcount/mr/mapper.py
   :language: python
   :pyobject: Mapper

.. literalinclude:: ../examples/self_contained/vowelcount/mr/reducer.py
   :language: python
   :pyobject: Reducer


How it Works
------------

The DC supports automatic distribution of files and archives across
the cluster at job launch time.  This feature can be used to dispatch
Python packages to all nodes, eliminating the need to install
dependencies for your application, including Pydoop itself::

  pydoop submit --upload-archive-to-cache vowelcount.tgz \
                --upload-archive-to-cache pydoop.tgz [...]

The ``pydoop.tgz`` and ``vowelcount.tgz`` archives will be copied to
all slave nodes and unpacked; in addition, ``pydoop`` and
``vowelcount`` symlinks will be created in the current working
directory of each task before it is executed.  If you include in each
archive the *contents* of the corresponding package, they will be
available for import::

  cd examples/self_contained/vowelcount
  tar cfz ../vowelcount.tgz .

The archive must be in one of the formats supported by Hadoop: zip, tar or tgz.

.. note::

  Pydoop submit automatically builds the name of the symlink that
  points to the unpacked archive by stripping the last extension.
  Thus, ``foo.tar.gz`` will not work as expected, since the link will
  be called ``foo.tar``. Always use the ``.tgz`` extension in this
  case.

The example is supposed to work with Pydoop and vowelcount *not*
installed on the slave nodes (you do need Pydoop on the client machine
used to run the example, however).
