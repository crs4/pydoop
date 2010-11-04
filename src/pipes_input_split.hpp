// BEGIN_COPYRIGHT
// END_COPYRIGHT

#ifndef HADOOP_PIPES_INPUT_SPLIT_HPP
#define HADOOP_PIPES_INPUT_SPLIT_HPP

#include <stdint.h>
#include <string>
#include "pipes_serial_utils.hpp"

namespace bp = boost::python;


struct wrap_input_split {
protected:
  std::string filename_;
  bp::long_ offset_, length_;
public:
  wrap_input_split(const std::string& raw_input_split) {
    _StringInStream is(raw_input_split);
#ifdef VINT_ISPLIT_FILENAME
    hu::deserializeString(filename_, is);
#else
    uint16_t fn_len = is.readUShort();
    char *buf = new char[fn_len];
    is.read(buf, fn_len);
    filename_ = buf;
    delete[] buf;
#endif
    offset_ = bp::long_(is.readLong());
    length_ = bp::long_(is.readLong());
  }
  std::string filename();
  bp::long_ offset();
  bp::long_ length();
};

#endif // HADOOP_PIPES_INPUT_SPLIT_HPP
