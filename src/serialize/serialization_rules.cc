/* BEGIN_COPYRIGHT
 *
 * Copyright 2009-2016 CRS4.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy
 * of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * END_COPYRIGHT
 */
#include "serialization_rules.hh"

#include <iostream>

// FIXME -- there are many Py_calls without checks on the return values.

PyObject* WritableRules_new(PyTypeObject *type,
                            PyObject *args, PyObject *kwds) {
  WritableRulesInfo *self;
  self = (WritableRulesInfo *)type->tp_alloc(type, 0);
  self->rules = new WritableRules();
  return (PyObject *)self;
}

void WritableRules_dealloc(WritableRulesInfo *self) {
  delete self->rules;
  Py_TYPE(self)->tp_free((PyObject*)self);
}

int WritableRules_init(WritableRulesInfo *self,
                       PyObject *args, PyObject *kwds) {
    return 0;
}

PyObject* WritableRules_add(WritableRulesInfo *self, PyObject *args) {
  static char *msg = "argument <PythonType>, <tuple of (<Bytes, Bytes>)> tuple.";
  if (!PyTuple_Check(args)) {
    PyErr_SetString(PyExc_ValueError, msg);
    return NULL;
  }
  if (PyTuple_GET_SIZE(args) != 2) {
    PyErr_SetString(PyExc_ValueError, msg);
    return NULL;
  }
  PyObject *type = PyTuple_GET_ITEM(args, 0);
  PyObject *list = PyTuple_GET_ITEM(args, 1);
  if (!PyType_Check(type)) {
    PyErr_SetString(PyExc_TypeError, msg);
    return NULL;
  }
  if (!PyTuple_Check(list)) {
    PyErr_SetString(PyExc_TypeError, msg);
    return NULL;
  }
  Py_ssize_t size = PyTuple_GET_SIZE(list);
  WritableRules::details_t *details = new WritableRules::details_t();
  details->reserve(size);
  for(Py_ssize_t pos = 0; pos < size; ++pos) {
    PyObject *tuple = PyTuple_GET_ITEM(list, pos);
    if (!PyTuple_Check(tuple) || PyTuple_GET_SIZE(tuple) != 2) {
      PyErr_SetString(PyExc_TypeError, msg);
      return NULL;
    }
    PyObject *prop = PyTuple_GET_ITEM(tuple, 0);
    PyObject *code = PyTuple_GET_ITEM(tuple, 1);    
    if (!PyBytes_Check(prop) || !PyBytes_Check(code)) {
      PyErr_SetString(PyExc_TypeError, msg);
      return NULL;
    }
    details->push_back(std::pair<std::string, std::string>
                       (std::string(PyBytes_AsString(prop)),
                        std::string(PyBytes_AsString(code))));
  }
  self->rules->add(type, details);
  Py_RETURN_NONE;
}

PyObject* WritableRules_rule(WritableRulesInfo *self,
                             PyObject *arg) {
  if (!PyType_Check(arg)) {
    PyErr_SetString(PyExc_ValueError,
                    "argument should be a <PythonType>.");
    return NULL;
  }
  if (!self->rules->has_rule(arg)) {
    Py_RETURN_NONE;
  }
  const WritableRules::details_t *details = self->rules->get_rule(arg);
  PyObject *tuple = PyTuple_New(details->size());
  for(WritableRules::details_t::const_iterator it = details->begin();
      it != details->end(); ++it) {
    std::size_t i = it - details->begin();
    PyObject *item = PyTuple_New(2);
    PyTuple_SET_ITEM(item, 0, PyBytes_FromString(it->first.c_str()));
    PyTuple_SET_ITEM(item, 1, PyBytes_FromString(it->second.c_str()));
    PyTuple_SET_ITEM(tuple, i, item);
  }
  return tuple;
}


