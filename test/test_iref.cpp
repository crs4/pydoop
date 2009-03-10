#include <boost/python.hpp>
#include <structmember.h>   // PyMemberDef

#include "custom_return_internal_reference.hpp"

class Inner
{};
struct PyInner
{
  PyObject_HEAD
  PyObject *ppyobjWeakrefList;
  Inner *pinner;
};

// New/Dealloc added to support weakref
PyObject * PyInnerNew (PyTypeObject *ptypeNew, PyObject *ppyobjArgs, PyObject 
*ppyobjKwds)
{
  PyObject *ppyobjNew = PyType_GenericNew(ptypeNew, ppyobjArgs, ppyobjKwds);

  if (ppyobjNew != NULL && !PyErr_Occurred()) // If created object ok
  {
    PyInner *ppyinnerNew = reinterpret_cast<PyInner *>(ppyobjNew);
    ppyinnerNew->ppyobjWeakrefList = NULL;
  }

  return ppyobjNew;
}

static void PyInnerDealloc(PyInner *ppyinnerDealloc)
{
  // Allocate temporaries if needed, but do not begin destruction just yet
  if (ppyinnerDealloc->ppyobjWeakrefList != NULL)
  {
    PyObject_ClearWeakRefs(reinterpret_cast<PyObject *>(ppyinnerDealloc));
  }
  ppyinnerDealloc->ob_type->tp_free(ppyinnerDealloc);
}

static PyMemberDef l_amemberPyInner[] =
{
    {"__weakref__", T_OBJECT, offsetof(PyInner, ppyobjWeakrefList), 0},
    {0}
};

PyTypeObject typeInner =
{
    PyObject_HEAD_INIT(NULL)
    0                                         // ob_size (?!?!?)
   ,"Inner"                                   // tp_name
   ,sizeof(PyInner)                           // tp_basicsize
   ,0                                         // tp_itemsize
   ,(destructor)&PyInnerDealloc               // tp_dealloc
   ,0                                         // tp_print
   ,0                                         // tp_getattr
   ,0                                         // tp_setattr
   ,0                                         // tp_compare
   ,0                                         // tp_repr
   ,0                                         // tp_as_number
   ,0                                         // tp_as_sequence
   ,0                                         // tp_as_mapping
   ,0                                         // tp_hash 
   ,0                                         // tp_call
   ,0                                         // tp_str
   ,0                                         // tp_getattro
   ,0                                         // tp_setattro
   ,0                                         // tp_as_buffer
   ,Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE  // tp_flags
   |Py_TPFLAGS_HAVE_WEAKREFS
   ,0                                         // tp_doc
   ,0                                         // tp_traverse
   ,0                                         // tp_clear
   ,0                                         // tp_richcompare
   ,offsetof(PyInner, ppyobjWeakrefList)      // tp_weaklistoffset
   ,0                                         // tp_iter
   ,0                                         // tp_iternext
   ,0                                         // tp_methods
   ,l_amemberPyInner                          // tp_members
   ,0                                         // tp_getset
   ,0                                         // tp_base
   ,0                                         // tp_dict
   ,0                                         // tp_descr_get
   ,0                                         // tp_descr_set
   ,0                                         // tp_dictoffset
   ,0                                         // tp_init
   ,0                                         // tp_alloc
   ,PyInnerNew                                // tp_new
};

struct convert_inner_to_python
{
  static PyObject * convert(Inner const &rinner)
  {
    PyInner *ppyinnerReturn = PyObject_New(PyInner, &typeInner);
    ppyinnerReturn->ppyobjWeakrefList = NULL;
    // The following line looks decidedly like a bad idea
    ppyinnerReturn->pinner = const_cast<Inner *>(&rinner); 
    PyObject *ppyobjReturn = reinterpret_cast <PyObject *>(ppyinnerReturn);
    return ppyobjReturn;
  }
};

struct Outer
{
 Inner m_inner;
};

namespace ConversionExample
{
void export_now()
{
  using namespace boost::python;
  using custom_call_policies::return_registered_internal_reference;
  typeInner.ob_type = &PyType_Type;
  PyType_Ready(&typeInner);
  to_python_converter<Inner, convert_inner_to_python>();
//  convert_python_to_inner();

  class_<Outer>("Outer")
    // This works but object lifetime is incorrect
    .add_property("innerval", make_getter(&Outer::m_inner, 
					  return_value_policy<return_by_value>()),
                              make_setter(&Outer::m_inner, 
					  return_value_policy<return_by_value>()))
    //This now also works
    .add_property("inner", make_getter(&Outer::m_inner, 
				       return_registered_internal_reference<1,default_call_policies>()),
                           make_setter(&Outer::m_inner, 
				       return_registered_internal_reference<1,default_call_policies>()))
    ;
}
} // End of namespace ConversionExample

namespace {
BOOST_PYTHON_MODULE(test_iref)
{
  ConversionExample::export_now();
}
} // Ends anonymous namespace
