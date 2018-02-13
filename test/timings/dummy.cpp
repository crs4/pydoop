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

#include <Python.h>

#include <string>
#include <map>
#include <iostream>
#include "SerialUtils.hh"

namespace hu = HadoopUtils;

// FIXME set an enum
#define MAP_ITEM 4
#define CLOSE 8

static PyObject *DummyError;

static PyObject *
dummy_decode_command2(PyObject *self, PyObject *args) {
  PyObject *po = PyTuple_GetItem(args, 0);
  if (!PyFile_Check(po)) {
    PyErr_SetString(DummyError, "First argument should be a file object.");
    return NULL;
  }
  hu::FileInStream stream;
  FILE *handle = PyFile_AsFile(po);
  stream.open(handle);
  
  int code = deserializeInt(stream);
  std::string key;
  std::string value;
  deserializeString(key, stream);
  deserializeString(value, stream);
  PyObject* res = PyTuple_New(3);
  PyTuple_SET_ITEM(res, 0, PyInt_FromLong(code));
  PyTuple_SET_ITEM(res, 1, PyString_FromStringAndSize(key.c_str(), key.size()));
  PyTuple_SET_ITEM(res, 2, PyString_FromStringAndSize(value.c_str(), value.size()));
  return res;
}

typedef std::map<int, std::string> cmd_encoding_t;
typedef cmd_encoding_t::mapped_type args_encoding_t;

class Decoder {
public:

  Decoder(const cmd_encoding_t& rules) : _rules(rules) {}

  inline PyObject* decode_cmd_from_stream(hu::InStream& stream) {
    int code;
    try {
      code = deserializeInt(stream);
    } catch (hu::Error& e) {
      // FIXME raise EOF if relevant...
      PyErr_SetString(DummyError, e.getMessage().c_str());        
      return NULL;
    }
    if (_rules.find(code) == _rules.end()) {
        PyErr_SetString(DummyError, "Unknown command code.");
        return NULL;
    }
    PyObject* args = decode_from_stream(_rules.at(code), stream);
    if (args == NULL) {
      return NULL;
    }
    PyObject* res = PyTuple_New(2);
    PyTuple_SET_ITEM(res, 0, PyInt_FromLong(code));
    PyTuple_SET_ITEM(res, 1, args);
    return res;
  }

  inline PyObject* decode_from_stream(const std::string& enc_rule, hu::InStream& stream) {
    PyObject* res = PyTuple_New(enc_rule.size());
    for(std::size_t i = 0; i < enc_rule.size(); ++i) {
      PyObject* v = decode_item(enc_rule[i], stream);
      if (v == NULL) {
        return NULL;
      }
      PyTuple_SET_ITEM(res, i, v);
    }
    return res;
  }
  inline PyObject* decode_from_buffer(const std::string& enc_rule, 
                                      const std::string buffer) {
    hu::StringInStream sis(buffer);
    return decode_from_stream(enc_rule, sis);
  }
  inline PyObject* decode_item(char code, hu::InStream& stream) {
    try {
      switch(code) {
      case 's':
        deserializeString(_buffer, stream);
        return PyString_FromStringAndSize(_buffer.c_str(), _buffer.size());
      case 'i':
        return PyInt_FromLong(deserializeInt(stream));
      default:
        PyErr_SetString(DummyError, "Unknown decoding code.");
        return NULL;
      } 
    } catch (hu::Error& e) {
      PyErr_SetString(DummyError, e.getMessage().c_str());        
      return NULL;
    }
  }

private:
  std::string _buffer;
  const cmd_encoding_t& _rules;
};

/* FIXME: There should be something simpler than this! */
struct A{
  static const cmd_encoding_t create_map() {
    cmd_encoding_t m;
    m[MAP_ITEM] = std::string("ss");
    m[CLOSE] = std::string("");
    return m;
  }
  static const cmd_encoding_t _map;
};

const cmd_encoding_t A::_map = A::create_map();

static Decoder decoder(A::_map);
/*
  dummy.decode_command_n(stream, N) -> ((cmd_code, args), .... (cmd_code_N, args_N))
 */
static PyObject *
dummy_decode_command(PyObject *self, PyObject *args) {
  int N = 1;

  PyObject *po = PyTuple_GetItem(args, 0);
  if (!PyFile_Check(po)) {
    PyErr_SetString(DummyError, "First argument should be  a file object.");
    return NULL;
  }
  if (PyTuple_Size(args) == 2) {
    PyObject *pn = PyTuple_GetItem(args, 1);
    N = PyInt_AS_LONG(pn);
  }

  hu::FileInStream stream;
  FILE *handle = PyFile_AsFile(po);
  stream.open(handle);

  PyObject* res = PyTuple_New(N);
  
  for(int i = 0;  i < N ; ++i) {
    PyObject* cmd = decoder.decode_cmd_from_stream(stream);
    if (cmd == NULL) {
      return NULL;
    }
    PyTuple_SET_ITEM(res, i, cmd);
    if (PyInt_AS_LONG(PyTuple_GetItem(cmd, 0)) == CLOSE) {
      _PyTuple_Resize(&res, i + 1);
      return res;
    }
  }
  return res;
}

/*
  dummy.decode_buffer(buffer, "") -> (args_tuple)
 */
static PyObject *
dummy_decode_buffer(PyObject *self, PyObject *args) {
}


static PyMethodDef DummyMethods[] = {
  {"decode_command", dummy_decode_command, METH_VARARGS,
   "Decode command from stream"},
  {"decode_command_old", dummy_decode_command2, METH_VARARGS,
   "Decode command from stream"},
  {NULL, NULL, 0, NULL}        /* Sentinel */
};

PyMODINIT_FUNC
initdummy(void)
{
  PyObject *m;
  m = Py_InitModule("dummy", DummyMethods);
  if (m == NULL) 
    return;
  DummyError = PyErr_NewException("dummy.error", NULL, NULL);
  Py_INCREF(DummyError);
  PyModule_AddObject(m, "error", DummyError);
}
