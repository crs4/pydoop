// BEGIN_COPYRIGHT
// END_COPYRIGHT

#ifndef HADOOP_PIPES_SERIAL_UTILS_HPP
#define HADOOP_PIPES_SERIAL_UTILS_HPP

#include <string>
#include "hadoop/SerialUtils.hh"
#include <boost/python.hpp>

namespace bp = boost::python;
namespace hu = HadoopUtils;


class _StringOutStream: public hu::OutStream {
protected:
  std::ostringstream _os;
public:
  _StringOutStream();
  void write(const void *buff, std::size_t len);
  void flush();
  std::string str();
};

class _StringInStream: public hu::InStream {
protected:
  std::istringstream _is; 
public:
  _StringInStream(const std::string& s);
  void read(void *buff, std::size_t len);
  uint16_t readUShort();
  uint64_t readLong();
  void seekg(std::size_t offset);
  std::size_t tellg();
};

std::string pipes_serialize_int(long t);
std::string pipes_serialize_float(float t);
std::string pipes_serialize_string(std::string t);
bp::tuple pipes_deserialize_int(const std::string& s, std::size_t offset);
bp::tuple pipes_deserialize_float(const std::string& s, std::size_t offset);
bp::tuple pipes_deserialize_string(const std::string& s, std::size_t offset);

#endif // HADOOP_PIPES_SERIAL_UTILS_HPP
