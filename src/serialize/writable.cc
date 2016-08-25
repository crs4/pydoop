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
#include "writable.hh"
#include "../py3k_compat.h"


static inline
PyObject* get_default_type(PyObject* o) {
  if (o != Py_None && !PyType_Check(o)) {
    PyErr_SetString(PyExc_ValueError,
                  "Argument should be a type.");
    return NULL;
  }
  return o;
}

static inline
const WritableRules* get_rules(PyObject* o) {
  // FIXME we assume that this has been already checked.
  WritableRulesInfo *self = (WritableRulesInfo*) o;
  return self->rules;
}


inline PyObject* insert_props(PyObject* obj,
                              const WritableRules::details_t *rule,
                              PyObject* data) {
  for(std::size_t i = 0; i < PyTuple_GET_SIZE(data); ++i) {
    int res = PyObject_SetAttrString(obj, rule->at(i).first.c_str(),
                                     PyTuple_GET_ITEM(data, i));
    if (res < 0) {
      PyErr_SetString(PyExc_ValueError, "Cannot set attribute.");
      return NULL;
    }
  }
  return obj;
}

inline
PyObject* create_obj(PyObject* type,
                     const WritableRules::details_t *rule,
                     PyObject* data) {
  PyObject* obj = PyObject_CallObject(type, NULL);
  if (obj == NULL) {
    return NULL;
  }
  if (insert_props(obj, rule, data) == NULL) {
    Py_DECREF(obj);
    return NULL;
  }
  return obj;
}

inline std::string build_srule(const WritableRules::details_t *rule) {
  std::string srule("");
  for(std::size_t i = 0; i < rule->size(); ++i) {
    srule += rule->at(i).second;
  }
  return srule;
}

inline
PyObject* extract_props(PyObject* data, PyObject* obj,
                        const WritableRules::details_t *rule) {
  assert(PyTuple_Check(data));
  assert(PyTuple_GET_SIZE(data) == rule->size());  
  for(std::size_t i = 0; i < rule->size(); ++i) {
    std::string aname = rule->at(i).first;
    // This returns a new reference.
    PyObject* item = PyObject_GetAttrString(obj, aname.c_str());
    if (item == NULL) {
      return NULL;
    }
    // SET_ITEM does not incref(item). So, when data will be garbage collected
    // item will be decref and gc if nobody else is using it.
    PyTuple_SET_ITEM(data, i, item);
  }
  return data;
}


PyObject* WritableReader::read(PyObject* type) {
  type = (type != Py_None)? type : _default_class;
  if (type == Py_None) {
    PyErr_SetString(PyExc_ValueError, "No default type defined.");
    return NULL;
  }
  if (!_rules->has_rule(type)) {
    PyErr_SetString(PyExc_ValueError, "No decoding rule for given type.");
    return NULL;
  }
  const WritableRules::details_t *rule = _rules->get_rule(type);
  std::string srule = build_srule(rule);
  PyObject* data = _flow_reader->read(srule);
  if (data  == NULL) { // failure in deserialize.
    return NULL; 
  }
  PyObject* res;
  if (rule->at(0).first.size() == 0) { // special case, no attributes
    assert(rule->size() == 1);
    assert(PyTuple_GET_SIZE(data) == 1);    
    res = PyTuple_GET_ITEM(data, 0);
    // otherwise it gets DECREFed when data is DECREFed
    Py_INCREF(res);
  } else {
    res = create_obj(type, rule, data);
  }
  Py_DECREF(data);
  return res;
}

PyObject* WritableWriter::write(PyObject* obj) {
  PyObject* otype = (PyObject*) obj->ob_type;
  if (!_rules->has_rule(otype)) {
    PyErr_SetString(PyExc_ValueError, "No decoding rule for given type.");
    return NULL;
  }
  const WritableRules::details_t *rule = _rules->get_rule(otype);
  PyObject* data = PyTuple_New(rule->size());
  if (data == NULL) {
    return NULL;
  }
  if (rule->size() == 1 && rule->at(0).first.size() == 0) {
    // special case,
    PyTuple_SET_ITEM(data, 0, obj);
    // we need to incref obj, since SET_ITEM attach obj to data but does not
    // incref(obj). Therefore, when data will be garbage collected it will
    // decref obj too behind python's back.
    Py_INCREF(obj);
  } else {
    if (extract_props(data, obj, rule) == NULL) {
      Py_DECREF(data);
      return NULL;
    }
  }
  std::string srule = build_srule(rule);
  return _flow_writer->write(data, srule);
}


#define CHECK_RESULT(o,m) \
if (o == NULL) {\
  return NULL;\
}

/*
  FIXME: no protection on data allocation. Do we need it?
 */
PyObject* WritableReader_new(PyTypeObject *type,
                             PyObject* args, PyObject* kwds) {
  static char *msg =
    "argument should be (<instream>, <sercore.WritableRules>, <PythonType>).";
  WritableReaderInfo *self;
  if (PyTuple_GET_SIZE(args) != 3) {
    PyErr_SetString(PyExc_ValueError, msg);
    return NULL;
  }
  FlowReader *flow_reader = FlowReader::make(PyTuple_GET_ITEM(args, 0));
  CHECK_RESULT(flow_reader, msg);
  const WritableRules *rules = get_rules(PyTuple_GET_ITEM(args, 1));
  CHECK_RESULT(rules, msg);
  PyObject* default_type = get_default_type(PyTuple_GetItem(args, 2));
  CHECK_RESULT(type, msg);
  self = (WritableReaderInfo *)type->tp_alloc(type, 0);
  self->reader = new WritableReader(flow_reader, rules, default_type);
  return (PyObject* )self;
}


void WritableReader_dealloc(WritableReaderInfo *self) {
  delete self->reader;
  Py_TYPE(self)->tp_free((PyObject*)self);
}

int WritableReader_init(WritableReaderInfo *self,
                       PyObject* args, PyObject* kwds) {
  return 0;
}

PyObject* WritableReader_read(WritableReaderInfo *self, PyObject* atype) {
  return self->reader->read(atype);
}

PyObject* WritableReader_close(WritableReaderInfo *self) {
  return self->reader->close();
}

PyObject* WritableWriter_new(PyTypeObject *type,
                             PyObject* args, PyObject* kwds) {
  static char *msg =
    "argument should be (<outstream>, <sercore.WritableRules>).";
  WritableWriterInfo *self;
  if (PyTuple_GET_SIZE(args) != 2) {
    PyErr_SetString(PyExc_ValueError, msg);
    return NULL;
  }
  // FIXME check difference with GetItem
  const WritableRules *rules = get_rules(PyTuple_GET_ITEM(args, 1));
  CHECK_RESULT(rules, msg);
  FlowWriter *flow_writer = FlowWriter::make(PyTuple_GET_ITEM(args, 0));
  CHECK_RESULT(flow_writer, msg);
  self = (WritableWriterInfo *)type->tp_alloc(type, 0);
  self->writer = new WritableWriter(flow_writer, rules);
  return (PyObject* )self;
}

void WritableWriter_dealloc(WritableWriterInfo *self) {
  delete self->writer;
  Py_TYPE(self)->tp_free((PyObject*)self);
}

int WritableWriter_init(WritableWriterInfo *self,
                       PyObject* args, PyObject* kwds) {
    return 0;
}

PyObject* WritableWriter_write(WritableWriterInfo *self, PyObject* arg) {
  return self->writer->write(arg);
}

PyObject* WritableWriter_flush(WritableWriterInfo *self) {
  return self->writer->flush();
}

PyObject* WritableWriter_close(WritableWriterInfo *self) {
  return self->writer->close();
}



