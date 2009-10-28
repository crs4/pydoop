// BEGIN_COPYRIGHT
// END_COPYRIGHT
#include "hdfs_common.hpp"


void hdfs_exception_translator(hdfs_exception const& x) {
  PyErr_SetString(PyExc_IOError, x.what());
}


//++++++++++++++++++++++++++++++//
// Exporting class definitions. //
//++++++++++++++++++++++++++++++//

using namespace boost::python;

void export_hdfs_common() {
  register_exception_translator<hdfs_exception>(hdfs_exception_translator);
}
