#include <string>

#include "hadoop/SerialUtils.hh"
#include <boost/python.hpp>

namespace bp = boost::python;
namespace hu = HadoopUtils;


class _StringOutStream: public hu::OutStream {
public:
  _StringOutStream() :  _os() {};
  void write(const void *buff, std::size_t len){
    _os.write(static_cast<const char*>(buff), len);
  }
  void flush() {}
  std::string str() { return _os.str(); }
protected:
  std::ostringstream _os;
};


class _StringInStream: public hu::InStream {
public:
  _StringInStream(const std::string& s) :  _is(s) {};
  void read(void *buff, std::size_t len){
    _is.read(static_cast<char*>(buff), len);
  }
  void seekg(std::size_t offset){
    _is.seekg(offset);
  }
  std::size_t tellg(){
    return _is.tellg();
  }
protected:
  std::istringstream _is; 
};


#define PIPES_SERIALIZE_DEF(type, name, hu_name) \
std::string name(type t) {                       \
  _StringOutStream os;                           \
  hu_name(t, os);                                \
  return os.str();                               \
}


PIPES_SERIALIZE_DEF(long, pipes_serialize_int, hu::serializeLong);
PIPES_SERIALIZE_DEF(float, pipes_serialize_float, hu::serializeFloat);
PIPES_SERIALIZE_DEF(std::string, pipes_serialize_string, hu::serializeString);


bp::tuple pipes_deserialize_int(const std::string& s, std::size_t offset) {
  _StringInStream is(s);
  is.seekg(offset);
  long i = hu::deserializeLong(is);
  return bp::make_tuple(is.tellg(), i);
}

bp::tuple pipes_deserialize_float(const std::string& s, std::size_t offset) {
  _StringInStream is(s);
  is.seekg(offset);
  float f = hu::deserializeFloat(is);
  return bp::make_tuple(is.tellg(), f);
}

bp::tuple pipes_deserialize_string(const std::string& s, std::size_t offset) {
  _StringInStream is(s);
  is.seekg(offset);
  std::string res;
  hu::deserializeString(res, is);
  return bp::make_tuple(is.tellg(), res);
}


//++++++++++++++++++++++++++++++//
// Exporting class definitions. //
//++++++++++++++++++++++++++++++//

using namespace boost::python;

void export_pipes_serial_utils() {
  def("serialize_int", pipes_serialize_int);
  def("serialize_float", pipes_serialize_float);
  def("serialize_string", pipes_serialize_string);
  def("deserialize_int", pipes_deserialize_int);
  def("deserialize_float", pipes_deserialize_float);
  def("deserialize_string", pipes_deserialize_string);
}
