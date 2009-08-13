#include "hdfs_common.hpp"

void hdfs_exception_translator(hdfs_exception const& x) {
  PyErr_SetString(PyExc_IOError, x.what());
}

void export_hdfs_common()
{
  using namespace boost::python;
  register_exception_translator<hdfs_exception>(hdfs_exception_translator);
}
