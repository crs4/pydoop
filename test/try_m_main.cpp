#include <boost/python.hpp>
void export_try_inheritance();
void export_try_reference();
BOOST_PYTHON_MODULE(try_m){
export_try_inheritance();
export_try_reference();
}
