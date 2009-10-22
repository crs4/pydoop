#ifndef HADOOP_HDFS_COMMON_HPP
#define HADOOP_HDFS_COMMON_HPP

#include <string>
#include <exception>

#include "hdfs.h"
#include <boost/python.hpp>

namespace bp = boost::python;


//+++++++++++++++++++++++++++++++++++++++++++++++++++++//
//                 hdfs_exception                      //
//+++++++++++++++++++++++++++++++++++++++++++++++++++++//

class hdfs_exception: public std::exception {
private:
  const std::string msg_;
public:
  hdfs_exception(std::string msg) : msg_(msg) {}
  virtual const char* what() const throw() {
    return msg_.c_str();
  }
  ~hdfs_exception() throw() {}
};

#endif // HADOOP_HDFS_COMMON_HPP
