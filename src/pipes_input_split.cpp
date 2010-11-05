// BEGIN_COPYRIGHT
// END_COPYRIGHT

#include "pipes_input_split.hpp"

namespace bp = boost::python;


std::string wrap_input_split::filename() {
  return filename_;
}

bp::long_ wrap_input_split::offset() {
  return offset_;
}

bp::long_ wrap_input_split::length() {
  return length_;
}


//++++++++++++++++++++++++++++++//
// Exporting class definitions. //
//++++++++++++++++++++++++++++++//

using namespace boost::python;

void export_input_split() {
  class_<wrap_input_split, boost::noncopyable>("input_split", init<std::string>())
    .add_property("filename", &wrap_input_split::filename)
    .add_property("offset", &wrap_input_split::offset)
    .add_property("length", &wrap_input_split::length)
    ;
}
