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

#ifndef PYDOOP_COMMAND_HH
#define PYDOOP_COMMAND_HH

#include <Python.h>
#include <structmember.h> 

#include <string>
#include <vector>
#include <assert.h>
#include "flow.hh"


PyObject* get_rules(void);


class CommandReader {
public:
  CommandReader(FlowReader* flow_reader) :
    _flow_reader(flow_reader) {}

  // returns tuple(CMD_CODE, tuple(args))
  PyObject* read(void) ;

  inline PyObject* close(void) { return _flow_reader->close();}

  ~CommandReader() {
    delete _flow_reader;
  }

private:
  FlowReader* _flow_reader;
};


class CommandWriter {
public:
  CommandWriter(FlowWriter* flow_writer)
    : _flow_writer(flow_writer) {}

  inline PyObject* flush(void) { return _flow_writer->flush(); }
  inline PyObject* close(void) { return _flow_writer->close(); }

  // tuple(CMD_CODE, tuple(args))
  inline PyObject* write(PyObject* args) ;

  ~CommandWriter() {
    delete _flow_writer;
  }

private:
  FlowWriter* _flow_writer;
};


typedef struct {
  PyObject_HEAD
  CommandReader* reader;
} CommandReaderInfo;


typedef struct {
  PyObject_HEAD
  CommandWriter* writer;
} CommandWriterInfo;


PyObject* CommandWriter_new(PyTypeObject *type, PyObject *args, PyObject *kwds);
int CommandWriter_init(CommandWriterInfo *self, PyObject *args, PyObject *kwds);
void CommandWriter_dealloc(CommandWriterInfo *self);
PyObject* CommandWriter_write(CommandWriterInfo *self, PyObject* args);
PyObject* CommandWriter_flush(CommandWriterInfo *self);
PyObject* CommandWriter_close(CommandWriterInfo *self);


PyObject* CommandReader_new(PyTypeObject *type, PyObject *args, PyObject *kwds);
int CommandReader_init(CommandReaderInfo *self, PyObject *args, PyObject *kwds);
void CommandReader_dealloc(CommandReaderInfo *self);
PyObject* CommandReader_read(CommandReaderInfo *self);
PyObject* CommandReader_close(CommandReaderInfo *self);
PyObject* CommandReader_iter(PyObject* self);
PyObject* CommandReader_iternext(PyObject* self);

#endif // PYDOOP_COMMAND_HH
