// BEGIN_COPYRIGHT
// END_COPYRIGHT

#include <hadoop/StringUtils.hh>
#include <hadoop/SerialUtils.hh>
#include <boost/python.hpp>
#include <string>

#include "utils.hpp"

namespace bp = boost::python;
namespace hu = HadoopUtils;


std::string deserialize_string(const std::string& s) {
  std::string ds;
  hu::StringInStream stream(s);
  hu::deserializeString(ds, stream);
  return ds;
}

std::string serialize_string(const std::string& s) {
  std::string ss;
  hu::StringOutStream stream(s);
  hu::deserializeString(ds, stream);
  return ds;
}


//++++++++++++++++++++++++++++++//
// Exporting class definitions. //
//++++++++++++++++++++++++++++++//

using namespace boost::python;

void export_utils() {
  def("deserialize_string", deserialize_string);
}
