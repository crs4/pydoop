New in 1.2.0
------------

 * Added support for Hadoop 2.7.2.
 * Dropped support for Python 2.6. Maintaining 2.6 compatibility would
   require adding another dimension to the Travis matrix, vastly
   increasing the build time and ultimately slowing down the
   development. Since the default Python version in all major
   distributions is 2.7, the added effort would gain us little.
 * Bug fixes.
