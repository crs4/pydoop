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

#define PY_SSIZE_T_CLEAN

#include <Python.h>

#include <string>
#include <memory>
#include <cstdlib>
#include <cstdint>
#include <cstdio>

#include "streams.h"


#if PY_MAJOR_VERSION >= 3
#define PyInt_FromLong PyLong_FromLong
#define PyInt_FromSize_t PyLong_FromSize_t
#define PyString_FromString PyBytes_FromString
#define PyString_FromStringAndSize PyBytes_FromStringAndSize
#define PyString_AsString PyBytes_AsString
#define PyString_AS_STRING PyBytes_AS_STRING
#define PyString_AsStringAndSize PyBytes_AsStringAndSize
#endif


// PyFile_AsFile is only available in Python 2, for "old style" file objects
// This should work on anything associated to a file descriptor
FILE *
_PyFile_AsFile(PyObject *f, const char* mode) {
  int fd, newfd;
  FILE *fp;
  if ((fd = PyObject_AsFileDescriptor(f)) == -1) {
    return NULL;
  }
  if ((newfd = dup(fd)) == -1) {
    PyErr_SetFromErrno(PyExc_IOError);
    return NULL;
  }
  if (!(fp = fdopen(newfd, mode))) {
    PyErr_SetFromErrno(PyExc_IOError);
    return NULL;
  }
  return fp;
}

static int
FileInStream_init(FileInStreamObj *self, PyObject *args, PyObject *kwds) {
  self->fp = NULL;
  self->stream = std::make_shared<HadoopUtils::FileInStream>();
  return 0;
}


static PyObject *
FileInStream_open(FileInStreamObj *self, PyObject *args) {
  const char *filename;
  if (PyArg_ParseTuple(args, "es", "utf-8", &filename)) {
    if (!self->stream->open(std::string(filename))) {
      PyMem_Free((void*)filename);
      return PyErr_SetFromErrno(PyExc_IOError);
    }
    PyMem_Free((void*)filename);
  } else {
    PyErr_Clear();
    PyObject *inarg;
    if (!PyArg_ParseTuple(args, "O", &inarg)) {
      return NULL;
    }
    if (!(self->fp = _PyFile_AsFile(inarg, "rb"))) {
      return NULL;
    }
    self->stream->open(self->fp);
  }
  Py_RETURN_NONE;
}


static PyObject *
FileInStream_close(FileInStreamObj *self) {
  if (self->fp) {
    fclose(self->fp);
  }
  if (!self->stream->close()) {
    return PyErr_SetFromErrno(PyExc_IOError);
  }
  Py_RETURN_NONE;
}


static PyObject *
FileInStream_read(FileInStreamObj *self, PyObject *args) {
  size_t len;
  PyObject *rval;
  PyThreadState *state;
  if (!PyArg_ParseTuple(args, "n", &len)) {
    return NULL;
  }
  if (!(rval = PyString_FromStringAndSize(NULL, len))) {
    return NULL;
  }
  state = PyEval_SaveThread();
  try {
    self->stream->read(PyString_AS_STRING(rval), len);
  } catch (HadoopUtils::Error e) {
    PyEval_RestoreThread(state);
    PyErr_SetString(PyExc_IOError, e.getMessage().c_str());
    Py_DECREF(rval);
    return NULL;
  }
  PyEval_RestoreThread(state);
  return rval;
}


static PyObject *
FileInStream_readInt(FileInStreamObj *self) {
  int32_t rval;
  PyThreadState *state;
  state = PyEval_SaveThread();
  try {
    rval = HadoopUtils::deserializeInt(*self->stream);
  } catch (HadoopUtils::Error e) {
    PyEval_RestoreThread(state);
    PyErr_SetString(PyExc_IOError, e.getMessage().c_str());
    return NULL;
  }
  PyEval_RestoreThread(state);
  return PyInt_FromLong(rval);
}


static PyObject *
FileInStream_readLong(FileInStreamObj *self) {
  int64_t rval;
  PyThreadState *state;
  state = PyEval_SaveThread();
  try {
    rval = HadoopUtils::deserializeLong(*self->stream);
  } catch (HadoopUtils::Error e) {
    PyEval_RestoreThread(state);
    PyErr_SetString(PyExc_IOError, e.getMessage().c_str());
    return NULL;
  }
  PyEval_RestoreThread(state);
  return PyLong_FromLong(rval);
}


static PyObject *
FileInStream_readFloat(FileInStreamObj *self) {
  float rval;
  PyThreadState *state;
  state = PyEval_SaveThread();
  try {
    rval = HadoopUtils::deserializeFloat(*self->stream);
  } catch (HadoopUtils::Error e) {
    PyEval_RestoreThread(state);
    PyErr_SetString(PyExc_IOError, e.getMessage().c_str());
    return NULL;
  }
  PyEval_RestoreThread(state);
  return PyFloat_FromDouble(rval);
}


static PyObject *
FileInStream_readString(FileInStreamObj *self) {
  std::string rval;
  PyThreadState *state;
  state = PyEval_SaveThread();
  try {
    HadoopUtils::deserializeString(rval, *self->stream);
  } catch (HadoopUtils::Error e) {
    PyEval_RestoreThread(state);
    PyErr_SetString(PyExc_IOError, e.getMessage().c_str());
    return NULL;
  }
  PyEval_RestoreThread(state);
  return PyUnicode_FromString(rval.c_str());
}


static PyObject *
FileInStream_readTuple(FileInStreamObj *self, PyObject *args) {
  char *fmt;
  PyObject *rval;
  if (!PyArg_ParseTuple(args, "s", &fmt)) {
    return NULL;
  }
  std::size_t nitems = strlen(fmt);
  if (!(rval = PyTuple_New(nitems))) {
    return NULL;
  }
  PyObject *item;
  for (std::size_t i = 0; i < nitems; ++i) {
    switch(fmt[i]) {
    case 'i':
      if (!(item = FileInStream_readInt(self))) {
	Py_DECREF(rval);
	return NULL;
      }
      PyTuple_SET_ITEM(rval, i, item);
      break;
    case 'l':
      if (!(item = FileInStream_readLong(self))) {
	Py_DECREF(rval);
	return NULL;
      }
      PyTuple_SET_ITEM(rval, i, item);
      break;
    case 'f':
      if (!(item = FileInStream_readFloat(self))) {
	Py_DECREF(rval);
	return NULL;
      }
      PyTuple_SET_ITEM(rval, i, item);
      break;
    case 's':
      if (!(item = FileInStream_readString(self))) {
	Py_DECREF(rval);
	return NULL;
      }
      PyTuple_SET_ITEM(rval, i, item);
      break;
    default:
      Py_DECREF(rval);
      return PyErr_Format(PyExc_ValueError, "Unknown format '%c'", fmt[i]);
    }
  }
  return rval;
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
  {"read_int", (PyCFunction)FileInStream_readInt, METH_NOARGS,
   "read_int(): read an integer from the stream"},
  {"read_long", (PyCFunction)FileInStream_readLong, METH_NOARGS,
   "read_long(): read a long integer from the stream"},
  {"read_float", (PyCFunction)FileInStream_readFloat, METH_NOARGS,
   "read_float(): read a float from the stream"},
  {"read_string", (PyCFunction)FileInStream_readString, METH_NOARGS,
   "read_string(): read a string from the stream"},
  {"read_tuple", (PyCFunction)FileInStream_readTuple, METH_VARARGS,
   "read_tuple(fmt): read len(fmt) values, where fmt specifies types"},
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
  self->fp = NULL;
  self->stream = std::make_shared<HadoopUtils::FileOutStream>();
  return 0;
}


static PyObject *
FileOutStream_open(FileOutStreamObj *self, PyObject *args) {
  const char *filename;
  if (PyArg_ParseTuple(args, "es", "utf-8", &filename)) {
    if (!self->stream->open(std::string(filename), true)) {
      PyMem_Free((void*)filename);
      return PyErr_SetFromErrno(PyExc_IOError);
    }
    PyMem_Free((void*)filename);
  } else {
    PyErr_Clear();
    PyObject *inarg;
    if (!PyArg_ParseTuple(args, "O", &inarg)) {
      return NULL;
    }
    if (!(self->fp = _PyFile_AsFile(inarg, "wb"))) {
      return NULL;
    }
    self->stream->open(self->fp);
  }
  Py_RETURN_NONE;
}


static PyObject *
FileOutStream_close(FileOutStreamObj *self) {
  if (self->fp) {
    fclose(self->fp);
  }
  if (!self->stream->close()) {
    return PyErr_SetFromErrno(PyExc_IOError);
  }
  Py_RETURN_NONE;
}


static PyObject *
FileOutStream_write(FileOutStreamObj *self, PyObject *args) {
  PyObject* data = NULL;
  Py_buffer buffer = {NULL, NULL};
  PyThreadState *state;
  if (!PyArg_ParseTuple(args, "O", &data)) {
    return NULL;
  }
  if (PyObject_GetBuffer(data, &buffer, PyBUF_SIMPLE) < 0) {
    PyErr_SetString(PyExc_TypeError, "data not accessible as a buffer");
    return NULL;
  }
  state = PyEval_SaveThread();
  try {
    self->stream->write(buffer.buf, buffer.len);
  } catch (HadoopUtils::Error e) {
    PyEval_RestoreThread(state);
    PyErr_SetString(PyExc_IOError, e.getMessage().c_str());
    return NULL;
  }
  PyEval_RestoreThread(state);
  Py_RETURN_NONE;
}


static PyObject *
FileOutStream_writeInt(FileOutStreamObj *self, PyObject *args) {
  int32_t val = 0;
  PyThreadState *state;
  if (!PyArg_ParseTuple(args, "n", &val)) {
    return NULL;
  }
  state = PyEval_SaveThread();
  try {
    HadoopUtils::serializeInt(val, *self->stream);
  } catch (HadoopUtils::Error e) {
    PyEval_RestoreThread(state);
    PyErr_SetString(PyExc_IOError, e.getMessage().c_str());
    return NULL;
  }
  PyEval_RestoreThread(state);
  Py_RETURN_NONE;
}


static PyObject *
FileOutStream_writeLong(FileOutStreamObj *self, PyObject *args) {
  int64_t val;
  PyThreadState *state;
  if (!PyArg_ParseTuple(args, "n", &val)) {
    return NULL;
  }
  state = PyEval_SaveThread();
  try {
    HadoopUtils::serializeLong(val, *self->stream);
  } catch (HadoopUtils::Error e) {
    PyEval_RestoreThread(state);
    PyErr_SetString(PyExc_IOError, e.getMessage().c_str());
    return NULL;
  }
  PyEval_RestoreThread(state);
  Py_RETURN_NONE;
}


static PyObject *
FileOutStream_writeFloat(FileOutStreamObj *self, PyObject *args) {
  float val;
  PyThreadState *state;
  if (!PyArg_ParseTuple(args, "f", &val)) {
    return NULL;
  }
  state = PyEval_SaveThread();
  try {
    HadoopUtils::serializeFloat(val, *self->stream);
  } catch (HadoopUtils::Error e) {
    PyEval_RestoreThread(state);
    PyErr_SetString(PyExc_IOError, e.getMessage().c_str());
    return NULL;
  }
  PyEval_RestoreThread(state);
  Py_RETURN_NONE;
}


static PyObject *
FileOutStream_writeString(FileOutStreamObj *self, PyObject *args) {
  char* val;
  PyThreadState *state;
  if (!PyArg_ParseTuple(args, "es", "utf-8",  &val)) {
    return NULL;
  }
  state = PyEval_SaveThread();
  try {
    HadoopUtils::serializeString(std::string(val), *self->stream);
  } catch (HadoopUtils::Error e) {
    PyEval_RestoreThread(state);
    PyErr_SetString(PyExc_IOError, e.getMessage().c_str());
    PyMem_Free((void*)val);
    return NULL;
  }
  PyEval_RestoreThread(state);
  PyMem_Free((void*)val);
  Py_RETURN_NONE;
}


static PyObject *
FileOutStream_advance(FileOutStreamObj *self, PyObject *args) {
  size_t len;
  if (!PyArg_ParseTuple(args, "n", &len)) {
    return NULL;
  }
  if (!self->stream->advance(len)) {
    return PyErr_SetFromErrno(PyExc_IOError);
  }
  Py_RETURN_NONE;
}


static PyObject *
FileOutStream_flush(FileOutStreamObj *self) {
  self->stream->flush();
  Py_RETURN_NONE;
}


static PyMethodDef FileOutStream_methods[] = {
  {"open", (PyCFunction)FileOutStream_open, METH_VARARGS,
   "open(filename): open file with the given name"},
  {"close", (PyCFunction)FileOutStream_close, METH_NOARGS,
   "close(): close the currently open file"},
  {"write", (PyCFunction)FileOutStream_write, METH_VARARGS,
   "write(data): write data to the stream"},
  {"write_int", (PyCFunction)FileOutStream_writeInt, METH_VARARGS,
   "write_int(n): write an integer to the stream"},
  {"write_long", (PyCFunction)FileOutStream_writeLong, METH_VARARGS,
   "write_long(n): write a long integer to the stream"},
  {"write_float", (PyCFunction)FileOutStream_writeFloat, METH_VARARGS,
   "write_float(n): write a float to the stream"},
  {"write_string", (PyCFunction)FileOutStream_writeString, METH_VARARGS,
   "write_string(n): write a string to the stream"},
  {"advance", (PyCFunction)FileOutStream_advance, METH_VARARGS,
   "advance(len): advance len bytes"},
  {"flush", (PyCFunction)FileOutStream_flush, METH_NOARGS,
   "flush(): flush the stream"},
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


static int
StringInStream_init(StringInStreamObj *self, PyObject *args, PyObject *kwds) {
  char *buf;
  Py_ssize_t len;
  if (!PyArg_ParseTuple(args, "s#", &buf, &len)) {
    return -1;
  }
  self->data = std::make_shared<std::string>(buf, len);
  self->stream = std::make_shared<HadoopUtils::StringInStream>(*self->data);
  return 0;
}


static PyObject *
StringInStream_read(StringInStreamObj *self, PyObject *args) {
  size_t len;
  PyObject *rval;
  PyThreadState *state;
  if (!PyArg_ParseTuple(args, "n", &len)) {
    return NULL;
  }
  if (len < 0 || len > self->data->size()) {
    len = self->data->size();  // as in io.BytesIO
  }
  if (!(rval = PyString_FromStringAndSize(NULL, len))) {
    return NULL;
  }
  state = PyEval_SaveThread();
  try {
    self->stream->read(PyString_AS_STRING(rval), len);
  } catch (HadoopUtils::Error e) {
    PyEval_RestoreThread(state);
    PyErr_SetString(PyExc_IOError, e.getMessage().c_str());
    Py_DECREF(rval);
    return NULL;
  }
  PyEval_RestoreThread(state);
  return rval;
}

static PyMethodDef StringInStream_methods[] = {
  {"read", (PyCFunction)StringInStream_read, METH_VARARGS,
   "read(len): read len bytes from the stream"},
  {NULL}
};


PyTypeObject StringInStreamType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "sercore.StringInStream",                         /* tp_name */
    sizeof(StringInStreamObj),                        /* tp_basicsize */
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
    "A stream that reads from a string",              /* tp_doc */
    0,                                                /* tp_traverse */
    0,                                                /* tp_clear */
    0,                                                /* tp_richcompare */
    0,                                                /* tp_weaklistoffset */
    0,                                                /* tp_iter */
    0,                                                /* tp_iternext */
    StringInStream_methods,                           /* tp_methods */
    0,                                                /* tp_members */
    0,                                                /* tp_getset */
    0,                                                /* tp_base */
    0,                                                /* tp_dict */
    0,                                                /* tp_descr_get */
    0,                                                /* tp_descr_set */
    0,                                                /* tp_dictoffset */
    (initproc)StringInStream_init,                    /* tp_init */
    0,                                                /* tp_alloc */
    0,                                                /* tp_new */
};
