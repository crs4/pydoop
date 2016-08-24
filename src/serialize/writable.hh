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
#ifndef PYDOOP_WRITABLE_HH
#define PYDOOP_WRITABLE_HH

#include <Python.h>
#include <structmember.h> // Python

#include <string>
#include <vector>
#include <assert.h>
#include "serialization_rules.hh"
#include "flow.hh"

class WritableReader {
public:
  WritableReader(FlowReader* flow_reader, const WritableRules* rules,
                 PyObject* default_class) :
    _flow_reader(flow_reader), _rules(rules), _default_class(default_class) {}

  PyObject* read(PyObject* type) ;

  inline PyObject* close(void) {
    // FIXME -- wrap potential errors.
    _flow_reader->close();
    Py_RETURN_NONE;
  }
  
  ~WritableReader() {
    delete _flow_reader;
  }

private:
  FlowReader* _flow_reader;
  const WritableRules* _rules;
  PyObject* _default_class;
};

class WritableWriter {
public:
  WritableWriter(FlowWriter* flow_writer, const WritableRules* rules) :
    _flow_writer(flow_writer), _rules(rules) {}

  inline PyObject* write(PyObject* obj) ;

  inline PyObject* flush(void) {
    // FIXME -- wrap potential errors.
    _flow_writer->flush();
    Py_RETURN_NONE;
  }
  
  inline PyObject* close(void) {
    // FIXME -- wrap potential errors.
    _flow_writer->close();
    Py_RETURN_NONE;
  }
  
  ~WritableWriter() {
    flush();
    delete _flow_writer;
  }
private:
  FlowWriter* _flow_writer;
  const WritableRules* _rules;
  const PyObject* _default_class;
};


typedef struct {
  PyObject_HEAD
  WritableReader* reader;
} WritableReaderInfo;


typedef struct {
  PyObject_HEAD
  WritableWriter* writer;
} WritableWriterInfo;


PyObject* WritableWriter_new(PyTypeObject *type, PyObject *args, PyObject *kwds);
int WritableWriter_init(WritableWriterInfo *self, PyObject *args, PyObject *kwds);
void WritableWriter_dealloc(WritableWriterInfo *self);
PyObject* WritableWriter_write(WritableWriterInfo *self, PyObject* args);
PyObject* WritableWriter_flush(WritableWriterInfo *self);
PyObject* WritableWriter_close(WritableWriterInfo *self);


PyObject* WritableReader_new(PyTypeObject *type, PyObject *args, PyObject *kwds);
int WritableReader_init(WritableReaderInfo *self, PyObject *args, PyObject *kwds);
void WritableReader_dealloc(WritableReaderInfo *self);
PyObject* WritableReader_read(WritableReaderInfo *self, PyObject* args);
PyObject* WritableReader_close(WritableReaderInfo *self);


#endif // PYDOOP_WRITABLE_HH
