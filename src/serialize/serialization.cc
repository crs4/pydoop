/* BEGIN_COPYRIGHT
 *
 * Copyright 2009-2018 CRS4.
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
#include "serialization.hh"
#include "SerialUtils.hh"
#include <errno.h>


#include "../py3k_compat.h"

inline
PyObject* handle_hu_error(hu::Error& e) {
  if (e.getMessage().find("end of file") != std::string::npos) {
    PyErr_SetString(PyExc_EOFError, e.getMessage().c_str());    
  } else {
    PyErr_SetString(PyExc_TypeError, e.getMessage().c_str());        
  }
  return NULL;
}

#define SERIALIZE_IN_THREADS(code) \
Py_BEGIN_ALLOW_THREADS; \
try {\
  code;\
} catch (hu::Error& e) {\
  Py_BLOCK_THREADS;\
  return handle_hu_error(e);\
}\
Py_END_ALLOW_THREADS\

inline
PyObject* serialize_item(hu::OutStream& stream,
                         char code, PyObject* o) {
  switch(code) {
  case 's': {
    Py_buffer buffer;
    if (PyObject_GetBuffer(o, &buffer, PyBUF_SIMPLE) < 0) {
      PyErr_SetString(PyExc_TypeError,
                      "Argument is not accessible as a Python buffer");
      return NULL;        
    } else {
      std::string s((char *) buffer.buf, buffer.len);
      SERIALIZE_IN_THREADS(serializeString(s, stream));
      return o; // everything is ok.
    }
  }
  case 'S': { // Special case, Hadoop WritableUtils
    if (o == Py_None) {
      std::string s;
      SERIALIZE_IN_THREADS(serializeWUString(s, true, stream));
      return o;
    }
    Py_buffer buffer;
    if (PyObject_GetBuffer(o, &buffer, PyBUF_SIMPLE) < 0) {
      PyErr_SetString(PyExc_TypeError,
                      "Argument is not accessible as a Python buffer");
      return NULL;        
    } else {
      std::string s((char *) buffer.buf, buffer.len);
      SERIALIZE_IN_THREADS(serializeWUString(s, false, stream));
      return o; // everything is ok.
    }
  }
  case 'i': {
    long v = PyInt_AsLong(o);
    if (v == -1 && PyErr_Occurred()) {
      return NULL;
    }
    SERIALIZE_IN_THREADS(serializeInt(v, stream));
    return o; // everything is ok.    
  }
  case 'L': {
    long long v = PyLong_AsLongLong(o);
    if (v == -1 && PyErr_Occurred()) {
      return NULL;
    }
    SERIALIZE_IN_THREADS(serializeLong(v, stream));
    return o; // everything is ok.    
  }
  case 'f': {
    float v = PyFloat_AsDouble(o);
    if (v == -1.0 && PyErr_Occurred()) {
      return NULL;
    }
    SERIALIZE_IN_THREADS(serializeFloat(v, stream));
    return o; // everything is ok.
  }
  case 'A': {
    if (!PyTuple_Check(o)) {
      PyErr_SetString(PyExc_TypeError,
                      "A argument should be a tuple.");
      return NULL;
    }
    Py_ssize_t n = PyTuple_GET_SIZE(o);
    SERIALIZE_IN_THREADS(serializeInt(n, stream));
    for(Py_ssize_t i = 0; i < n; ++i){
      PyObject *item = serialize_item(stream, 's', PyTuple_GET_ITEM(o, i));
      if (item == NULL) {
        return NULL;
      }
    }
    return o;
  }
  default:
    PyErr_SetString(PyExc_TypeError, "Unknown decoding code.");
    return NULL;
  }
}

#define DESERIALIZE_IN_THREADS(code) SERIALIZE_IN_THREADS(code)

inline
PyObject* deserialize_item(hu::InStream& stream, char code) {
  switch(code) {
  case 's': {
    std::string buffer;
    DESERIALIZE_IN_THREADS(deserializeString(buffer, stream));
    return _PyBuf_FromStringAndSize(buffer.c_str(), buffer.size());
  }
  case 'S': { // Special case, Hadoop WritableUtils
    std::string buffer;
    bool is_empty;
    DESERIALIZE_IN_THREADS(deserializeWUString(buffer, is_empty, stream));
    if (is_empty) {
      Py_RETURN_NONE;
    }
    return _PyBuf_FromStringAndSize(buffer.c_str(), buffer.size());
  }
  case 'i': {
    long v;
    DESERIALIZE_IN_THREADS(v = deserializeInt(stream));
    return PyLong_FromLong(v);
  }
  case 'L': {
    long long v;
    DESERIALIZE_IN_THREADS(v = deserializeLong(stream));    
    return PyLong_FromLongLong(v);
  }
  case 'f': {
    float v;
    DESERIALIZE_IN_THREADS(deserializeFloat(v, stream));    
    return PyFloat_FromDouble((double)v);
  }
  case 'A': {
      Py_ssize_t n;
      DESERIALIZE_IN_THREADS(n = deserializeInt(stream));
      PyObject* res = PyTuple_New(n);        
      for(Py_ssize_t i = 0; i < n; ++i){
        PyObject* item = deserialize_item(stream, 's');
        if (item == NULL) {
          return NULL;
        }
        PyTuple_SET_ITEM(res, i, item);
      }
      return res;
  }
  default:
    PyErr_SetString(PyExc_TypeError, "Unknown decoding code.");
    return NULL;
  }
}


PyObject* serialize_int(hu::OutStream* stream, PyObject* code) {
  return serialize_item(*stream, 'i', code);
}

PyObject* deserialize_int(hu::InStream* stream) {
  return deserialize_item(*stream, 'i');
}  

PyObject* serialize(hu::OutStream* stream,  const std::string& srule,
                    const PyObject* data) {
  assert(PyTuple_Check(data));
  assert(PyTuple_GET_SIZE(data) == (int)srule.size());
  for(std::size_t i = 0; i < srule.size(); ++i) {
    PyObject* res = serialize_item(*stream, srule[i], PyTuple_GET_ITEM(data, i));
    if (res == NULL) {
      return NULL;
    }
  }
  Py_INCREF(Py_None);
  return Py_None;
}
  

PyObject* deserialize(hu::InStream* stream, const std::string& srule) {
  PyObject* result = PyTuple_New(srule.size());
  for(std::size_t i = 0; i < srule.size(); ++i) {
    PyObject *item = deserialize_item(*stream, srule[i]);
    if (item == NULL) {
      // Note that garbage collecting result will garbage collect all the tuple
      // items.
      Py_DECREF(result);
      return NULL;
    }
    // Note that the SET_ITEM steals a ref from item
    PyTuple_SET_ITEM(result, i, item);
  }
  return result;
}
