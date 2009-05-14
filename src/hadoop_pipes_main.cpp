#include <boost/python.hpp>
void export_hadoop_pipes();
void export_hadoop_pipes_context();
void export_hadoop_pipes_test_support();
BOOST_PYTHON_MODULE(hadoop_pipes){
export_hadoop_pipes();
export_hadoop_pipes_context();
export_hadoop_pipes_test_support();
}
