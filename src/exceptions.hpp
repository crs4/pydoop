#ifndef HADOOP_EXCEPTIONS_HPP
#define HADOOP_EXCEPTIONS_HPP

#include <boost/python.hpp>

namespace bp = boost::python;


class pydoop_exception: public std::exception {
private:
  const std::string msg_;
public:
  pydoop_exception(std::string msg) : msg_(msg) {}
  virtual const char* what() const throw() {
    return msg_.c_str();
  }
  virtual ~pydoop_exception() throw() {}
};


class pipes_exception: public pydoop_exception {
public:
  pipes_exception(std::string msg) : pydoop_exception(msg) {}
  bp::tuple args(void) {
    return bp::make_tuple(what());
  }
};

#endif // HADOOP_EXCEPTIONS_HPP
