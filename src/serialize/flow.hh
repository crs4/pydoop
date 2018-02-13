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
#ifndef PYDOOP_FLOW_HH
#define PYDOOP_FLOW_HH

#include <Python.h>

#include <string>
#include <assert.h>

#include "../py3k_compat.h"

#include "serialization.hh"

namespace hu = HadoopUtils;

class FlowReader {

public:
  static FlowReader* make(PyObject* o);
  
public:
  FlowReader(hu::InStream* stream) : _stream(stream) {}

  inline PyObject* skip(std::size_t nbytes) {
    if (!_stream->skip(nbytes)) {
      PyErr_SetString(PyExc_TypeError,
                      "Failed skip");
      return NULL;
    }
    Py_RETURN_NONE;
  }

  inline PyObject* read(const std::string& rule) {
    return deserialize(_stream, rule);
  }

  inline PyObject* read_int(void) {
    return deserialize_int(_stream);
  }
  
  inline PyObject* close(void) {
    _stream->close();
    Py_RETURN_NONE;
  }

  ~FlowReader() {
    delete _stream;
  }

private:
  hu::InStream* _stream;
};

class FlowWriter {

public:
  static FlowWriter* make(PyObject* o);

public:
  FlowWriter(hu::OutStream* stream) : _stream(stream) {}

  inline PyObject* write(const std::string& rule, PyObject* data) {
    return serialize(_stream, rule, data);
  }
  
  inline PyObject* write_int(PyObject* v) {
    return serialize_int(_stream, v);
  }

  inline PyObject* flush(void) {
    // FIXME -- wrap potential errors.
    _stream->flush();
    Py_RETURN_NONE;
  }

  inline PyObject* close(void) {
    _stream->close();
    Py_RETURN_NONE;
  }
  
  ~FlowWriter() {
    delete _stream;
  }

private:
  hu::OutStream* _stream;
};

typedef struct {
  PyObject_HEAD
  FlowReader* reader;
} FlowReaderInfo;


typedef struct {
  PyObject_HEAD
  FlowWriter* writer;
} FlowWriterInfo;


PyObject* FlowWriter_new(PyTypeObject *type, PyObject *args, PyObject *kwds);
int FlowWriter_init(FlowWriterInfo *self, PyObject *args, PyObject *kwds);
void FlowWriter_dealloc(FlowWriterInfo *self);
PyObject* FlowWriter_write(FlowWriterInfo *self, PyObject* args);
PyObject* FlowWriter_flush(FlowWriterInfo *self);
PyObject* FlowWriter_close(FlowWriterInfo *self);


PyObject* FlowReader_new(PyTypeObject *type, PyObject *args, PyObject *kwds);
int FlowReader_init(FlowReaderInfo *self, PyObject *args, PyObject *kwds);
void FlowReader_dealloc(FlowReaderInfo *self);
PyObject* FlowReader_read(FlowReaderInfo *self, PyObject *arg);
PyObject* FlowReader_skip(FlowReaderInfo *self, PyObject *arg);
PyObject* FlowReader_close(FlowReaderInfo *self);
PyObject* FlowReader_iter(PyObject* self);
PyObject* FlowReader_iternext(PyObject* self);



#endif // PYDOOP_FLOW_HH
