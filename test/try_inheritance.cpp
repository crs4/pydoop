#include <boost/python.hpp>
#include <boost/python/call.hpp>

#include <iostream>

using namespace boost::python;

#include <string>

struct Base {
  virtual ~Base() {}
  virtual int         f() = 0;
  virtual float       g() = 0;
  virtual std::string h() = 0;
  virtual const std::string& sref() = 0;
};


struct BaseWrap : Base, wrapper<Base>{
  int f() {
    return this->get_override("f")();
  }
  float g() {
    return this->get_override("g")();
  }
  std::string h() {
    return this->get_override("h")();
  }
  const std::string& sref() {
    return this->get_override("sref")();
  }
};

int check_base_int(Base& b){
  return b.f();
}
float check_base_float(Base& b){
  return b.g();
}
std::string check_base_string(Base& b){
  return b.h();
}

std::string check_base_sref(Base& b){
  const std::string& s = b.sref();
  //return std::string(b.sref());
  return std::string("ddd");
}


struct A {
  virtual const std::string& getName() { return name_;}
  void  setName(const std::string & n) { name_ = n;}
  void internal_check() {
    std::cerr << "internal check has been called" << std::endl;
    std::cerr << "internal check returns " << this->getName() << std::endl;
  }
private:
  std::string name_;
};

struct AWrap : A, wrapper<A>{
  const std::string& getName() {
    if (override f = this->get_override("getName")) {
#if 0
      return call<const std::string&>(f.ptr());
#else
      object c = f();
      return extract<const std::string&>(boost::ref(c));
      //char const* cp =  extract<char const*>(boost::ref(c));
      
#endif
      //const std::string& x = call<const std::string&>(f.ptr());
      //return x;
    } else {
      return A::getName();
    }
  }
  const std::string& default_getName() { return this->A::getName();}
};

std::string check_a(A& a){
  return std::string(a.getName());
}


void export_try_inheritance() 
{

  class_<BaseWrap, boost::noncopyable>("Base")
    .def("f", pure_virtual(&Base::f))
    .def("g", pure_virtual(&Base::g))
    .def("h", pure_virtual(&Base::h))
    .def("sref", &BaseWrap::sref,
	 return_value_policy<copy_const_reference>())
    ;
  def("check_base_int", check_base_int);
  def("check_base_float", check_base_float);
  def("check_base_string", check_base_string);
  def("check_base_sref", check_base_sref);

  /*
  class_<A>("A")
    .def("getName", &A::getName,
	 return_value_policy<copy_const_reference>())
    .def("setName", &A::setName)
    ;
  */
  class_<AWrap, boost::noncopyable>("A")
    .def("getName", &A::getName,
	 &AWrap::default_getName,
	 return_value_policy<copy_const_reference>())
    .def("setName", &A::setName)
    .def("internal_check", &A::internal_check)
    ;
  def("check_a", check_a);
    
}


