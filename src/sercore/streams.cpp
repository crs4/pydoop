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

#include <Python.h>

#include <string>
#include <memory>
#include <cstdlib>

#include "streams.h"


#if PY_MAJOR_VERSION >= 3
#define PyInt_FromSize_t PyLong_FromSize_t
#define PyString_FromString PyBytes_FromString
#define PyString_FromStringAndSize PyBytes_FromStringAndSize
#endif


static int
FileInStream_init(FileInStreamObj *self, PyObject *args, PyObject *kwds) {
  self->stream = std::make_shared<HadoopUtils::FileInStream>();
  return 0;
}


static PyObject *
FileInStream_open(FileInStreamObj *self, PyObject *args) {
  const char *filename;
  if (!PyArg_ParseTuple(args, "s", &filename)) {
    return NULL;
  }
  if (!self->stream->open(std::string(filename))) {
    return PyErr_SetFromErrno(PyExc_IOError);
  }
  Py_RETURN_NONE;
}


static PyObject *
FileInStream_close(FileInStreamObj *self) {
  if (!self->stream->close()) {
    return PyErr_SetFromErrno(PyExc_IOError);
  }
  Py_RETURN_NONE;
}


static PyObject *
FileInStream_read(FileInStreamObj *self, PyObject *args) {
  size_t len;
  if (!PyArg_ParseTuple(args, "n", &len)) {
    return NULL;
  }
  char* buf = reinterpret_cast<char*>(malloc(len));
  try {
    self->stream->read(buf, len);
  } catch (const std::exception& e) {
    PyErr_SetString(PyExc_IOError, e.what());
    return NULL;
  }
  return PyString_FromStringAndSize(buf, len);
}


static PyObject *
FileInStream_skip(FileInStreamObj *self, PyObject *args) {
  size_t len;
  if (!PyArg_ParseTuple(args, "n", &len)) {
    return NULL;
  }
  if (!self->stream->skip(len)) {
    return PyErr_SetFromErrno(PyExc_IOError);
  }
  Py_RETURN_NONE;
}

static PyMethodDef FileInStream_methods[] = {
  {"open", (PyCFunction)FileInStream_open, METH_VARARGS,
   "open(filename): open file with the given name"},
  {"close", (PyCFunction)FileInStream_close, METH_NOARGS,
   "close(): close the currently open file"},
  {"read", (PyCFunction)FileInStream_read, METH_VARARGS,
   "read(len): read len bytes from the stream"},
  {"skip", (PyCFunction)FileInStream_skip, METH_VARARGS,
   "skip(len): skip len bytes"},
  {NULL}  /* Sentinel */
};


PyTypeObject FileInStreamType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "sercore.FileInStream",                           /* tp_name */
    sizeof(FileInStreamObj),                          /* tp_basicsize */
    0,                                                /* tp_itemsize */
    0,                                                /* tp_dealloc */
    0,                                                /* tp_print */
    0,                                                /* tp_getattr */
    0,                                                /* tp_setattr */
    0,                                                /* tp_compare */
    0,                                                /* tp_repr */
    0,                                                /* tp_as_number */
    0,                                                /* tp_as_sequence */
    0,                                                /* tp_as_mapping */
    0,                                                /* tp_hash */
    0,                                                /* tp_call */
    0,                                                /* tp_str */
    0,                                                /* tp_getattro */
    0,                                                /* tp_setattro */
    0,                                                /* tp_as_buffer */
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,         /* tp_flags */
    "A class to read a file as a stream",             /* tp_doc */
    0,                                                /* tp_traverse */
    0,                                                /* tp_clear */
    0,                                                /* tp_richcompare */
    0,                                                /* tp_weaklistoffset */
    0,                                                /* tp_iter */
    0,                                                /* tp_iternext */
    FileInStream_methods,                             /* tp_methods */
    0,                                                /* tp_members */
    0,                                                /* tp_getset */
    0,                                                /* tp_base */
    0,                                                /* tp_dict */
    0,                                                /* tp_descr_get */
    0,                                                /* tp_descr_set */
    0,                                                /* tp_dictoffset */
    (initproc)FileInStream_init,                      /* tp_init */
    0,                                                /* tp_alloc */
    0,                                                /* tp_new */
};


static int
FileOutStream_init(FileOutStreamObj *self, PyObject *args, PyObject *kwds) {
  self->stream = std::make_shared<HadoopUtils::FileOutStream>();
  return 0;
}


static PyObject *
FileOutStream_open(FileOutStreamObj *self, PyObject *args) {
  const char *filename;
  if (!PyArg_ParseTuple(args, "s", &filename)) {
    return NULL;
  }
  if (!self->stream->open(std::string(filename), true)) {
    return PyErr_SetFromErrno(PyExc_IOError);
  }
  Py_RETURN_NONE;
}


static PyObject *
FileOutStream_close(FileOutStreamObj *self) {
  if (!self->stream->close()) {
    return PyErr_SetFromErrno(PyExc_IOError);
  }
  Py_RETURN_NONE;
}


static PyObject *
FileOutStream_write(FileOutStreamObj *self, PyObject *args) {
  PyObject* data = NULL;
  Py_buffer buffer = {NULL, NULL};
  if (!PyArg_ParseTuple(args, "O", &data)) {
    return NULL;
  }
  if (PyObject_GetBuffer(data, &buffer, PyBUF_SIMPLE) < 0) {
    PyErr_SetString(PyExc_TypeError, "data not accessible as a buffer");
    return NULL;
  }
  try {
    self->stream->write(buffer.buf, buffer.len);
  } catch (const std::exception& e) {
    PyErr_SetString(PyExc_IOError, e.what());
    return NULL;
  }
  Py_RETURN_NONE;
}


static PyMethodDef FileOutStream_methods[] = {
  {"open", (PyCFunction)FileOutStream_open, METH_VARARGS,
   "open(filename): open file with the given name"},
  {"close", (PyCFunction)FileOutStream_close, METH_NOARGS,
   "close(): close the currently open file"},
  {"write", (PyCFunction)FileOutStream_write, METH_VARARGS,
   "write(data): write data to the stream"},
  {NULL}  /* Sentinel */
};


PyTypeObject FileOutStreamType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "sercore.FileOutStream",                          /* tp_name */
    sizeof(FileOutStreamObj),                         /* tp_basicsize */
    0,                                                /* tp_itemsize */
    0,                                                /* tp_dealloc */
    0,                                                /* tp_print */
    0,                                                /* tp_getattr */
    0,                                                /* tp_setattr */
    0,                                                /* tp_compare */
    0,                                                /* tp_repr */
    0,                                                /* tp_as_number */
    0,                                                /* tp_as_sequence */
    0,                                                /* tp_as_mapping */
    0,                                                /* tp_hash */
    0,                                                /* tp_call */
    0,                                                /* tp_str */
    0,                                                /* tp_getattro */
    0,                                                /* tp_setattro */
    0,                                                /* tp_as_buffer */
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,         /* tp_flags */
    "A class to write a stream to a file",            /* tp_doc */
    0,                                                /* tp_traverse */
    0,                                                /* tp_clear */
    0,                                                /* tp_richcompare */
    0,                                                /* tp_weaklistoffset */
    0,                                                /* tp_iter */
    0,                                                /* tp_iternext */
    FileOutStream_methods,                            /* tp_methods */
    0,                                                /* tp_members */
    0,                                                /* tp_getset */
    0,                                                /* tp_base */
    0,                                                /* tp_dict */
    0,                                                /* tp_descr_get */
    0,                                                /* tp_descr_set */
    0,                                                /* tp_dictoffset */
    (initproc)FileOutStream_init,                     /* tp_init */
    0,                                                /* tp_alloc */
    0,                                                /* tp_new */
};
