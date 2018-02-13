// BEGIN_COPYRIGHT
//
// Copyright 2009-2018 CRS4.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy
// of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.
//
// END_COPYRIGHT

#include "command.hh"
#include "SerialUtils.hh"

#include <map>

typedef std::map<int, const std::string> rules_map_t;

static rules_map_t rules;

/*
# these constants should be exactly what has been defined in
# PipesMapper.java and BinaryProtocol.java
*/
enum cmd_code { 
  START_MESSAGE = 0,
  SET_JOB_CONF = 1,
  SET_INPUT_TYPES = 2,
  RUN_MAP = 3,
  MAP_ITEM = 4,
  RUN_REDUCE = 5,
  REDUCE_KEY = 6,
  REDUCE_VALUE = 7,
  CLOSE = 8,
  ABORT = 9,
  AUTHENTICATION_REQ = 10,
  OUTPUT = 50,
  PARTITIONED_OUTPUT = 51,
  STATUS = 52,
  PROGRESS = 53,
  DONE = 54,
  REGISTER_COUNTER = 55,
  INCREMENT_COUNTER = 56,
  AUTHENTICATION_RESP = 57,
};


// FIXME this is not thread safe. We will probably not need it to be.
static void load_rules_if_empty() {
  if (rules.size() > 0) { return; }
  rules.insert(rules_map_t::value_type(START_MESSAGE, "i"));
  rules.insert(rules_map_t::value_type(SET_JOB_CONF, "A"));
  rules.insert(rules_map_t::value_type(SET_INPUT_TYPES, "ss"));
  rules.insert(rules_map_t::value_type(RUN_MAP, "sii"));
  rules.insert(rules_map_t::value_type(MAP_ITEM, "ss"));
  rules.insert(rules_map_t::value_type(RUN_REDUCE, "ii"));
  rules.insert(rules_map_t::value_type(REDUCE_KEY, "s"));
  rules.insert(rules_map_t::value_type(REDUCE_VALUE, "s"));
  rules.insert(rules_map_t::value_type(CLOSE, ""));
  rules.insert(rules_map_t::value_type(ABORT, ""));
  rules.insert(rules_map_t::value_type(AUTHENTICATION_REQ, "ss"));
  rules.insert(rules_map_t::value_type(OUTPUT, "ss"));
  rules.insert(rules_map_t::value_type(PARTITIONED_OUTPUT, "iss"));
  rules.insert(rules_map_t::value_type(STATUS, "s"));
  rules.insert(rules_map_t::value_type(PROGRESS, "f"));
  rules.insert(rules_map_t::value_type(DONE, ""));
  rules.insert(rules_map_t::value_type(REGISTER_COUNTER, "iss"));
  rules.insert(rules_map_t::value_type(INCREMENT_COUNTER, "iL"));
  rules.insert(rules_map_t::value_type(AUTHENTICATION_RESP, "s"));
}


PyObject* CommandReader::read(void) {
  /*
    Notice that we detect possible EOF (and other major catastrophes) by a NULL
    pcode.
   */
  PyObject* pcode = _flow_reader->read_int();
  if (pcode == NULL) {
    return NULL;
  }
  int code = PyInt_AsLong(pcode);
  if (code == -1 && PyErr_Occurred()) {
    Py_DECREF(pcode);
    return NULL;
  }
  if (rules.find(code) == rules.end()) {
    PyErr_SetString(PyExc_TypeError, "unexpected rule code.");
    Py_DECREF(pcode);
    return NULL;
  }
  const std::string& rule = rules[code];
  PyObject* args = _flow_reader->read(rule);
  if (args  == NULL) {
    Py_DECREF(pcode);
    return NULL; 
  }
  PyObject* result = PyTuple_New(2);
  if (result == NULL) {
    Py_DECREF(pcode);
    return NULL; 
  }
  PyTuple_SET_ITEM(result, 0, pcode);
  PyTuple_SET_ITEM(result, 1, args);  
  return result;
}

PyObject* CommandWriter::write(PyObject* targs) {
  if(!PyTuple_Check(targs) || PyTuple_GET_SIZE(targs) != 2) {
    PyErr_SetString(PyExc_TypeError,
                    "badly formed argument");
  }
  PyObject* pcode = PyTuple_GET_ITEM(targs, 0);
  int code = PyInt_AsLong(pcode);
  if (code == -1 && PyErr_Occurred()) {
    PyErr_SetString(PyExc_TypeError,
                    "bad command code type.");
    return NULL;
  }
  if (rules.find(code) == rules.end()) {
    PyErr_SetString(PyExc_TypeError, "unexpected cmd code.");
    return NULL;
  }
  if (_flow_writer->write_int(pcode) == NULL) {
    return NULL;
  }
  return _flow_writer->write(rules[code], PyTuple_GET_ITEM(targs, 1));
}

//   
#define CHECK_RESULT(o,m) \
if (o == NULL) {\
  return NULL;\
}

/*
  FIXME: no protection on data allocation. Do we need it?
 */
PyObject* CommandReader_new(PyTypeObject *type,
                            PyObject *args, PyObject *kwds) {
  static char *msg =
    "argument should be <instream>.";
  CommandReaderInfo *self;
  if (PyTuple_GET_SIZE(args) != 1) {
    PyErr_SetString(PyExc_ValueError, msg);
    return NULL;
  }
  FlowReader *flow_reader = FlowReader::make(PyTuple_GET_ITEM(args, 0));
  CHECK_RESULT(flow_reader, msg);
  self = (CommandReaderInfo *)type->tp_alloc(type, 0);
  self->reader = new CommandReader(flow_reader);
  load_rules_if_empty();
  return (PyObject *)self;
}


void CommandReader_dealloc(CommandReaderInfo *self) {
  delete self->reader;
  Py_TYPE(self)->tp_free((PyObject*)self);
}

int CommandReader_init(CommandReaderInfo *self,
                       PyObject *args, PyObject *kwds) {
  return 0;
}

PyObject* CommandReader_read(CommandReaderInfo *self) {
  return self->reader->read();
}

PyObject* CommandReader_close(CommandReaderInfo *self) {
  return self->reader->close();
}

PyObject* CommandReader_iternext(PyObject *self) {
  PyObject* res = ((CommandReaderInfo *)self)->reader->read();
  if (res == NULL && PyErr_ExceptionMatches(PyExc_EOFError)) {
    PyErr_SetNone(PyExc_StopIteration);    
  }
  return res;
}

PyObject* CommandReader_iter(PyObject *self) {
  Py_INCREF(self);  
  return self;
}


PyObject* CommandWriter_new(PyTypeObject *type,
                            PyObject *args, PyObject *kwds) {
  static char *msg =
    "argument should be <outstream>.";
  if (PyTuple_GET_SIZE(args) != 1) {
    PyErr_SetString(PyExc_ValueError, msg);
    return NULL;
  }
  CommandWriterInfo *self;
  FlowWriter *flow_writer = FlowWriter::make(PyTuple_GET_ITEM(args, 0));
  CHECK_RESULT(flow_writer, msg);
  self = (CommandWriterInfo *)type->tp_alloc(type, 0);
  self->writer = new CommandWriter(flow_writer);
  load_rules_if_empty();  
  return (PyObject *)self;
}

void CommandWriter_dealloc(CommandWriterInfo *self) {
  delete self->writer;
  Py_TYPE(self)->tp_free((PyObject*)self);
}

int CommandWriter_init(CommandWriterInfo *self,
                       PyObject *args, PyObject *kwds) {
    return 0;
}

PyObject* CommandWriter_write(CommandWriterInfo *self, PyObject* args) {
  return self->writer->write(args);
}

PyObject* CommandWriter_flush(CommandWriterInfo *self) {
  return self->writer->flush();
}

PyObject* CommandWriter_close(CommandWriterInfo *self) {
  return self->writer->close();
}


PyObject* get_rules(void) {
  load_rules_if_empty();
  PyObject* py_rules = PyDict_New();
  for (rules_map_t::iterator it = rules.begin(); it != rules.end(); it++) {
    PyDict_SetItem(
      py_rules,
      PyLong_FromLong(it->first),
      PyUnicode_FromString((it->second).c_str()));
  }
  return py_rules;
}
