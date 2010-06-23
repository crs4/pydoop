Taking pydoop with you
======================

There are situations were one cannot expect to be able to install
arbitrary python packages <explain...>

This is an example on how one could harness the cache mechanims
provided by hadoop to do per-application distribution.

Prepare package
---------------

<explain...>::
  
  bash$ make foobar

will create a `foobar.tgz` with all the pydoop required modules and
your payload library foobar::

  bash$ tar tvzf foobar.tgz
  ...
  ./pydoop
  ./foobar

A minimal program that with ....


