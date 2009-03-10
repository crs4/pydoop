#include <boost/python.hpp>
#include <boost/python/self.hpp>
#include <iostream>

using namespace boost::python;

#include <string>
#include <vector>

class a_v_thing {
public:
  virtual int f() const = 0;
  virtual const std::string& g() const = 0;
  virtual ~a_v_thing() {}
};

class factory {
public:
  virtual a_v_thing* create(int v) = 0;
  virtual            ~factory(){};
};

class a_thing : public a_v_thing {
private:
  int x_;
  std::string y_;
public:
  a_thing(int x): x_(x), y_("foobar"){}
  int f() const { return x_; }
  const std::string& g() const { return y_; }
};

class a_box {
private:
  const a_v_thing& at_;
public:
  a_box(const a_v_thing& at): at_(at) { }
  int f() { return at_.f();}
};

void call_factory(factory& f, std::size_t n){
  for(std::size_t i = 0; i < n; ++i){
    a_v_thing* a = f.create(i);
    std::cerr << "a_v_thing iteration " << i 
	      << " produced " << a->f()
	      << ", " << a->g()
	      << std::endl;
  }
}

//--------------------------------------------------------
static std::vector<std::string> foo_dump;

struct a_v_thing_wrap: a_v_thing, 
		       wrapper<a_v_thing>{
  int f() const{
    return this->get_override("f")();
  }
  const std::string& g() const {
    str r = this->get_override("g")();
    foo_dump.push_back(extract<std::string>(r));
    return foo_dump.back();
  }
};

struct factory_wrap: factory,
		     wrapper<factory>{
  a_v_thing* create(int v) {
    // FIXME we need the concrete python class to
    // keep track of the produced objects, otherwise
    // the following will raise a dangling pointer exception.
    a_v_thing* av = this->get_override("create")(v);
    return av;
  }
};


void export_try_reference()
{
  class_<a_v_thing_wrap, boost::noncopyable>("a_v_thing")
    .def("f", pure_virtual(&a_v_thing::f))
    .def("g", pure_virtual(&a_v_thing::g),
	 return_value_policy<copy_const_reference>())
    ;
  class_<factory_wrap, boost::noncopyable>("factory")
    .def("create", pure_virtual(&factory::create),
	 return_value_policy<manage_new_object>())
    ;
  class_<a_thing, bases<a_v_thing> >("a_thing",
				     init<int>())
    .def("f", &a_thing::f)
    .def("g", &a_thing::g,
	 return_value_policy<copy_const_reference>())
    ;
  class_<a_box>("a_box",
		init<a_v_thing&>())
    .def("f", &a_box::f)
    ;
  def("call_factory", call_factory);
}

