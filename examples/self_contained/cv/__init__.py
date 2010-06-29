# BEGIN_COPYRIGHT
# END_COPYRIGHT
"""
This is a trivial example application that shows how to use the Hadoop
distributed cache to distribute Python libraries (possibly including
Pydoop itself) to all cluster nodes at job launch time. This is useful
in all cases where installing to each node is not feasible (e.g., lack
of a shared mount point).

Check the Makefile to see how you can accomplish this.
"""

# NOTE: some of the variables defined here are
# parsed by setup.py, check it before modifying them.

__version__ = "0.1"

__author__ = "Simone Leo, Gianluigi Zanetti"

__author_email__ = "<simone.leo@crs4.it>, <gianluigi.zanetti@crs4.it>"

__url__ = "http://pydoop.sourceforge.net"
