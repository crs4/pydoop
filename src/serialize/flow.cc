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

#include "flow.hh"

/*
  FIXME This is an ad hoc kludge to avoid duplicating buffers.
 */
class PyBufferInStream: public hu::BufferInStream {
public:
  PyBufferInStream() : hu::BufferInStream(){}
  bool open(PyObject *o) {
    if (PyObject_GetBuffer(o, &buffer, PyBUF_SIMPLE) == 0) {
      return hu::BufferInStream::open((char*)buffer.buf, buffer.len);
    } else {
      return false;
    }
  }
  virtual ~PyBufferInStream() {
    PyBuffer_Release(&buffer);
  }
private:
  Py_buffer buffer;
};

static inline
hu::InStream* get_in_stream(PyObject *o) {
  hu::InStream *stream;
  if (PyObject_CheckBuffer(o)) {
    PyBufferInStream *pstream = new PyBufferInStream();
    pstream->open(o);
    stream = pstream;
  } else {
    int fd = -1;
    char *msg = "Argument should be either a file or a buffer.";
#if IS_PY3K
    fd = PyObject_AsFileDescriptor(o);
    if (fd < 0) {
      PyErr_SetString(PyExc_ValueError, msg);
      return NULL;
    }
#else
    if (!PyFile_Check(o)) {
      PyErr_SetString(PyExc_ValueError, msg);
      return NULL;
    }
    fd = fileno(PyFile_AsFile(o)); // FIXME, this is ugly.
#endif
    FILE* fin = fdopen(fd, "rb");
    if (fin == NULL) {
      PyErr_SetString(PyExc_ValueError, "Cannot open file for reading.");
      return NULL;
    }
    hu::FileInStream *fstream = new hu::FileInStream();
    fstream->open(fin);
    stream = fstream;
  }
  return stream;
}


static inline
hu::OutStream* get_out_stream(PyObject *o) {
  hu::OutStream *stream;
  FILE *fout = NULL;
#if IS_PY3K
  int fd = PyObject_AsFileDescriptor(o);
  if (fd < 0) {
    PyErr_SetString(PyExc_ValueError, "First argument should be a file.");
    return NULL;
  }
#if 0
  int fddup = dup(fd);
  if (fddup < 0) {
    PyErr_SetString(PyExc_ValueError, "Cannot dup fd.");
    return NULL;
  }
  // FIXME!
  fout = fdopen(fddup, "wb");
#else
  fout = fdopen(fd, "wb");  
#endif
#else
  if (!PyFile_Check(o)) {
    PyErr_SetString(PyExc_ValueError, "First argument should be a file.");
    return NULL;
  }
  fout = PyFile_AsFile(o);
#endif
  if (fout == NULL) {
    PyErr_SetString(PyExc_ValueError, "Cannot open file for writing.");
    return NULL;
  }
  hu::FileOutStream *fstream = new hu::FileOutStream();
  fstream->open(fout);
  stream = fstream;
  return stream;
}

FlowReader* FlowReader::make(PyObject* o) {
  hu::InStream* stream = get_in_stream(o);
  if (stream == NULL) {
    return NULL;
  }
  return new FlowReader(stream);
}

FlowWriter* FlowWriter::make(PyObject* o) {
  hu::OutStream* stream = get_out_stream(o);
  if (stream == NULL) {
    return NULL;
  }
  return new FlowWriter(stream);
}

//   
#define CHECK_RESULT(o,m) \
if (o == NULL) {\
  return NULL;\
}

/*
  FIXME: no protection on data allocation. Do we need it?
 */
PyObject* FlowReader_new(PyTypeObject *type,
                            PyObject *args, PyObject *kwds) {
  static char *msg =
    "argument should be <instream>.";
  FlowReaderInfo *self;
  if (PyTuple_GET_SIZE(args) != 1) {
    PyErr_SetString(PyExc_ValueError, msg);
    return NULL;
  }
  FlowReader *flow_reader = FlowReader::make(PyTuple_GET_ITEM(args, 0));
  CHECK_RESULT(flow_reader, msg);
  self = (FlowReaderInfo *)type->tp_alloc(type, 0);
  self->reader = flow_reader;
  return (PyObject *)self;
}


void FlowReader_dealloc(FlowReaderInfo *self) {
  delete self->reader;
  Py_TYPE(self)->tp_free((PyObject*)self);
}

int FlowReader_init(FlowReaderInfo *self,
                    PyObject *args, PyObject *kwds) {
  return 0;
}


PyObject* FlowReader_skip(FlowReaderInfo *self, PyObject *arg) { // single arg
  long v = PyInt_AsLong(arg);
  if (v == -1 && PyErr_Occurred()) {
    return NULL;
  } else {
    assert(v >= 0);
    return self->reader->skip((std::size_t) v);
  }
}


PyObject* FlowReader_read(FlowReaderInfo *self, PyObject *arg) { // single arg
  Py_buffer buffer;
  if (PyObject_GetBuffer(arg, &buffer, PyBUF_SIMPLE) < 0) {
    PyErr_SetString(PyExc_TypeError,
                    "Argument is not accessible as a Python buffer");
    return NULL;        
  } else {
    std::string s((char *) buffer.buf, buffer.len);
    return self->reader->read(s);
  }
}

PyObject* FlowReader_close(FlowReaderInfo *self) {
  return self->reader->close();
}


PyObject* FlowWriter_new(PyTypeObject *type,
                         PyObject *args, PyObject *kwds) {
  static char *msg =
    "argument should be <outstream>.";
  if (PyTuple_GET_SIZE(args) != 1) {
    PyErr_SetString(PyExc_ValueError, msg);
    return NULL;
  }
  FlowWriterInfo *self;
  FlowWriter *flow_writer = FlowWriter::make(PyTuple_GET_ITEM(args, 0));
  CHECK_RESULT(flow_writer, msg);
  self = (FlowWriterInfo *)type->tp_alloc(type, 0);
  self->writer = flow_writer;
  return (PyObject *)self;
}

void FlowWriter_dealloc(FlowWriterInfo *self) {
  delete self->writer;
  Py_TYPE(self)->tp_free((PyObject*)self);
}

int FlowWriter_init(FlowWriterInfo *self,
                    PyObject *args, PyObject *kwds) {
    return 0;
}

PyObject* FlowWriter_write(FlowWriterInfo *self, PyObject* args) {
  assert(PyTuple_GET_SIZE(args) == 2);
  Py_buffer buffer;
  if (PyObject_GetBuffer(PyTuple_GET_ITEM(args, 0), &buffer, PyBUF_SIMPLE) < 0) {
    PyErr_SetString(PyExc_TypeError,
                    "First argument is not accessible as a Python buffer");
    return NULL;
  } else {
    std::string rule((char *) buffer.buf, buffer.len);
    return self->writer->write(rule, PyTuple_GET_ITEM(args, 1));
  }
}

PyObject* FlowWriter_flush(FlowWriterInfo *self) {
  return self->writer->flush();
}

PyObject* FlowWriter_close(FlowWriterInfo *self) {
  return self->writer->close();
}
