This is a trivial example application that shows how to use the Hadoop
distributed cache to distribute Python libraries (possibly including
Pydoop itself) to all cluster nodes at job launch time. This is useful
in all cases where installing to each node is not feasible (e.g., lack
of a shared mount point).

Check the Makefile to see how you can accomplish this.
