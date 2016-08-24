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
    fd = fileno(PyFile_AsFILE(o)); // FIXME, this is ugly.
#endif
    int fddup = dup(fd);
    if (fddup < 0) {
      PyErr_SetString(PyExc_ValueError, "Cannot dup fd.");
      return NULL;
    }
    FILE* fin = fdopen(fddup, "r");    
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
  fout = fdopen(fd, "w");
#else
  if (!PyFile_Check(po)) {
    PyErr_SetString(PyExc_ValueError, "First argument should be a file.");
    return NULL;
  }
  fout = PyFile_AsFILE(o);
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
