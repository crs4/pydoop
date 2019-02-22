// BEGIN_COPYRIGHT
//
// Copyright 2009-2019 CRS4.
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

// WARNING: types defined here are **NOT** designed for inheritance. For
// instance, FileInStream_readTuple calls other FileInStream_read* methods
// directly at the C++ level. Since they are not part of the public API ---
// exactly one input and one output stream are used in the pipes protocol, and
// that's it --- we can make the code simpler and more efficient.

#define PY_SSIZE_T_CLEAN  // must be defined before including Python.h

#include <Python.h>

#include <string>
#include <memory>
#include <cstdlib>
#include <cstdint>
#include <cstdio>

#include "hu_extras.h"
#include "streams.h"

#define OUTPUT 50
#define PARTITIONED_OUTPUT 51


// This can only be used in functions that return a PyObject*
# define _ASSERT_STREAM_OPEN {                                           \
  if (self->closed) {                                                    \
    PyErr_SetString(PyExc_ValueError, "I/O operation on closed stream"); \
    return NULL;                                                         \
  }                                                                      \
}

// PyFile_AsFile is only available in Python 2, for "old style" file objects
// This should work on anything associated to a file descriptor
FILE *
_PyFile_AsFile(PyObject *f, const char* mode) {
  int fd, newfd;
  FILE *fp;
  PyThreadState *state;
  if ((fd = PyObject_AsFileDescriptor(f)) == -1) {
    return NULL;
  }
  state = PyEval_SaveThread();
  if ((newfd = dup(fd)) == -1) {
    goto error;
  }
  if (!(fp = fdopen(newfd, mode))) {
    goto error;
  }
  PyEval_RestoreThread(state);
  return fp;

error:
    PyEval_RestoreThread(state);
    PyErr_SetFromErrno(PyExc_IOError);
    return NULL;
}


static int
FileInStream_init(FileInStreamObj *self, PyObject *args, PyObject *kwds) {
  const char *filename;
  PyThreadState *state;
  self->stream = std::make_shared<HadoopUtils::FileInStream>();
  if (PyArg_ParseTuple(args, "es", "utf-8", &filename)) {
    state = PyEval_SaveThread();
    if (!self->stream->open(std::string(filename))) {
      PyEval_RestoreThread(state);
      PyErr_SetFromErrno(PyExc_IOError);
      PyMem_Free((void*)filename);
      return -1;
    }
    PyEval_RestoreThread(state);
    PyMem_Free((void*)filename);
  } else {
    PyErr_Clear();
    PyObject *inarg;
    if (!PyArg_ParseTuple(args, "O", &inarg)) {
      return -1;
    }
    if (!(self->fp = _PyFile_AsFile(inarg, "rb"))) {
      return -1;
    }
    self->stream->open(self->fp);  // this variant just stores a reference
  }
  self->closed = false;
  return 0;
}


static PyObject *
FileInStream_close(FileInStreamObj *self) {
  PyThreadState *state;
  if (self->closed) {
    Py_RETURN_NONE;
  }
  state = PyEval_SaveThread();
  if (self->fp) {
    fclose(self->fp);
  }
  bool res = self->stream->close();
  if (!res) {
    PyEval_RestoreThread(state);
    return PyErr_SetFromErrno(PyExc_IOError);
  }
  PyEval_RestoreThread(state);
  self->closed = true;
  Py_RETURN_NONE;
}


static PyObject *
FileInStream_enter(FileInStreamObj *self) {
  _ASSERT_STREAM_OPEN;
  Py_INCREF(self);
  return (PyObject*)self;
}


static PyObject *
FileInStream_exit(FileInStreamObj *self, PyObject *args) {
  return FileInStream_close(self);
}


static PyObject *
FileInStream_read(FileInStreamObj *self, PyObject *args) {
  size_t len;
  PyObject *rval;
  PyThreadState *state;
  _ASSERT_STREAM_OPEN;
  if (!PyArg_ParseTuple(args, "n", &len)) {
    return NULL;
  }
  if (!(rval = PyBytes_FromStringAndSize(NULL, len))) {
    return NULL;
  }
  state = PyEval_SaveThread();
  try {
    self->stream->read(PyBytes_AS_STRING(rval), len);
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
FileInStream_readVInt(FileInStreamObj *self) {
  int32_t rval;
  PyThreadState *state;
  _ASSERT_STREAM_OPEN;
  state = PyEval_SaveThread();
  try {
    rval = HadoopUtils::deserializeInt(*self->stream);
  } catch (HadoopUtils::Error e) {
    PyEval_RestoreThread(state);
    PyErr_SetString(PyExc_IOError, e.getMessage().c_str());
    return NULL;
  }
  PyEval_RestoreThread(state);
  return Py_BuildValue("i", rval);
}


static PyObject *
FileInStream_readVLong(FileInStreamObj *self) {
  int64_t rval;
  PyThreadState *state;
  _ASSERT_STREAM_OPEN;
  state = PyEval_SaveThread();
  try {
    rval = HadoopUtils::deserializeLong(*self->stream);
  } catch (HadoopUtils::Error e) {
    PyEval_RestoreThread(state);
    PyErr_SetString(PyExc_IOError, e.getMessage().c_str());
    return NULL;
  }
  PyEval_RestoreThread(state);
  return Py_BuildValue("L", rval);
}


static PyObject *
FileInStream_readFloat(FileInStreamObj *self) {
  float rval;
  PyThreadState *state;
  _ASSERT_STREAM_OPEN;
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


std::string
_FileInStream_read_cppstring(FileInStreamObj *self) {
  std::string rval;
  PyThreadState *state;
  state = PyEval_SaveThread();
  try {
    HadoopUtils::deserializeString(rval, *self->stream);
  } catch (HadoopUtils::Error e) {
    PyEval_RestoreThread(state);
    PyErr_SetString(PyExc_IOError, e.getMessage().c_str());
    throw;
  }
  PyEval_RestoreThread(state);
  return rval;
}


static PyObject *
FileInStream_readString(FileInStreamObj *self) {
  _ASSERT_STREAM_OPEN;
  std::string s;
  try {
    s = _FileInStream_read_cppstring(self);
  } catch (HadoopUtils::Error e) {
    return NULL;
  }
  return PyUnicode_FromStringAndSize(s.c_str(), s.size());
}


static PyObject *
FileInStream_readBytes(FileInStreamObj *self) {
  _ASSERT_STREAM_OPEN;
  std::string s;
  try {
    s = _FileInStream_read_cppstring(self);
  } catch (HadoopUtils::Error e) {
    return NULL;
  }
  return PyBytes_FromStringAndSize(s.c_str(), s.size());
}


static PyObject *
FileInStream_readTuple(FileInStreamObj *self, PyObject *args) {
  char *fmt;
  PyObject *rval;
  _ASSERT_STREAM_OPEN;
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
      if (!(item = FileInStream_readVInt(self))) goto error;
      break;
    case 'l':
      if (!(item = FileInStream_readVLong(self))) goto error;
      break;
    case 'f':
      if (!(item = FileInStream_readFloat(self))) goto error;
      break;
    case 's':
      if (!(item = FileInStream_readString(self))) goto error;
      break;
    case 'b':
      if (!(item = FileInStream_readBytes(self))) goto error;
      break;
    default:
      Py_DECREF(rval);
      return PyErr_Format(PyExc_ValueError, "Unknown format '%c'", fmt[i]);
    }
    PyTuple_SET_ITEM(rval, i, item);
  }
  return rval;

error:
  Py_DECREF(rval);
  return NULL;
}


static PyObject *
FileInStream_skip(FileInStreamObj *self, PyObject *args) {
  size_t len;
  PyThreadState *state;
  _ASSERT_STREAM_OPEN;
  if (!PyArg_ParseTuple(args, "n", &len)) {
    return NULL;
  }
  state = PyEval_SaveThread();
  bool res = self->stream->skip(len);
  if (!res) {
    PyEval_RestoreThread(state);
    return PyErr_SetFromErrno(PyExc_IOError);
  }
  PyEval_RestoreThread(state);
  Py_RETURN_NONE;
}


// Extra types not used directly by the protocol, but that may appear as a
// result of serializing objects such as keys, values and input splits.
// **NOTE**: within the command stream, each serialized object starts with a
// VInt that specifies its length. For instance, to read a LongWritable key:
//     assert stream.read_vint() == 8
//     key = stream.read_long_writable()
// Equivalent, but probably less efficient:
//     key_bytes = stream.read_bytes()
//     assert len(key_bytes) == 8
//     key = struct.unpack(">q", key_bytes)[0]

static PyObject *
FileInStream_readLongWritable(FileInStreamObj *self) {
  int64_t rval = 0;
  PyThreadState *state;
  _ASSERT_STREAM_OPEN;
  state = PyEval_SaveThread();
  try {
    rval = deserializeLongWritable(*self->stream);
  } catch (HadoopUtils::Error e) {
    PyEval_RestoreThread(state);
    PyErr_SetString(PyExc_IOError, e.getMessage().c_str());
    return NULL;
  }
  PyEval_RestoreThread(state);
  return Py_BuildValue("L", rval);
}


static PyMethodDef FileInStream_methods[] = {
  {"close", (PyCFunction)FileInStream_close, METH_NOARGS,
   "close(): close the currently open file"},
  {"read", (PyCFunction)FileInStream_read, METH_VARARGS,
   "read(len): read len bytes from the stream"},
  {"read_vint", (PyCFunction)FileInStream_readVInt, METH_NOARGS,
   "read_vint(): read a variable length integer from the stream"},
  {"read_vlong", (PyCFunction)FileInStream_readVLong, METH_NOARGS,
   "read_vlong(): read a variable length long integer from the stream"},
  {"read_float", (PyCFunction)FileInStream_readFloat, METH_NOARGS,
   "read_float(): read a float from the stream"},
  {"read_string", (PyCFunction)FileInStream_readString, METH_NOARGS,
   "read_string(): read a string from the stream"},
  {"read_bytes", (PyCFunction)FileInStream_readBytes, METH_NOARGS,
   "read_bytes(): read a bytes object from the stream"},
  {"read_tuple", (PyCFunction)FileInStream_readTuple, METH_VARARGS,
   "read_tuple(fmt): read len(fmt) values, where fmt specifies types"},
  {"skip", (PyCFunction)FileInStream_skip, METH_VARARGS,
   "skip(len): skip len bytes"},
  {"__enter__", (PyCFunction)FileInStream_enter, METH_NOARGS},
  {"__exit__", (PyCFunction)FileInStream_exit, METH_VARARGS},
  {"read_long_writable", (PyCFunction)FileInStream_readLongWritable,
   METH_NOARGS, "read_long_writable(): read a hadoop.io.LongWritable"},
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
  const char *filename;
  PyThreadState *state;
  self->stream = std::make_shared<HadoopUtils::FileOutStream>();
  if (PyArg_ParseTuple(args, "es", "utf-8", &filename)) {
    state = PyEval_SaveThread();
    if (!self->stream->open(std::string(filename), true)) {
      PyEval_RestoreThread(state);
      PyErr_SetFromErrno(PyExc_IOError);
      PyMem_Free((void*)filename);
      return -1;
    }
    PyEval_RestoreThread(state);
    PyMem_Free((void*)filename);
  } else {
    PyErr_Clear();
    PyObject *inarg;
    if (!PyArg_ParseTuple(args, "O", &inarg)) {
      return -1;
    }
    if (!(self->fp = _PyFile_AsFile(inarg, "wb"))) {
      return -1;
    }
    self->stream->open(self->fp);  // this variant just stores a reference
  }
  self->closed = false;
  return 0;
}


static PyObject *
FileOutStream_close(FileOutStreamObj *self) {
  PyThreadState *state;
  if (self->closed) {
    Py_RETURN_NONE;
  }
  state = PyEval_SaveThread();
  if (self->fp) {
    fclose(self->fp);
  }
  bool res = self->stream->close();
  if (!res) {
    PyEval_RestoreThread(state);
    return PyErr_SetFromErrno(PyExc_IOError);
  }
  PyEval_RestoreThread(state);
  self->closed = true;
  Py_RETURN_NONE;
}


static PyObject *
FileOutStream_enter(FileOutStreamObj *self) {
  _ASSERT_STREAM_OPEN;
  Py_INCREF(self);
  return (PyObject*)self;
}


static PyObject *
FileOutStream_exit(FileOutStreamObj *self, PyObject *args) {
  return FileOutStream_close(self);
}


static PyObject *
FileOutStream_write(FileOutStreamObj *self, PyObject *args) {
  PyObject* data = NULL;
  Py_buffer buffer = {NULL, NULL};
  PyThreadState *state;
  _ASSERT_STREAM_OPEN;
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
FileOutStream_writeVInt(FileOutStreamObj *self, PyObject *args) {
  int val = 0;
  PyThreadState *state;
  _ASSERT_STREAM_OPEN;
  if (!PyArg_ParseTuple(args, "i", &val)) {
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
FileOutStream_writeVLong(FileOutStreamObj *self, PyObject *args) {
  long long val;
  PyThreadState *state;
  _ASSERT_STREAM_OPEN;
  if (!PyArg_ParseTuple(args, "L", &val)) {
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
  _ASSERT_STREAM_OPEN;
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


static PyObject*
_FileOutStream_write_cppstring(FileOutStreamObj *self, std::string s) {
  PyThreadState *state = PyEval_SaveThread();
  try {
    HadoopUtils::serializeString(s, *self->stream);
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
  _ASSERT_STREAM_OPEN;
#if PY_MAJOR_VERSION < 3
  // default encoding is ASCII, so "s#" would not work here
  PyObject *pystring, *pybytes;
  if (!PyArg_ParseTuple(args, "O", &pystring)) {
    return NULL;
  }
  std::string s;
  if (PyBytes_Check(pystring)) {
    s = std::string(PyBytes_AS_STRING(pystring), PyBytes_GET_SIZE(pystring));
  } else {
    if (!(pybytes = PyUnicode_AsUTF8String(pystring))) {
      return NULL;
    }
    s = std::string(PyBytes_AS_STRING(pybytes), PyBytes_GET_SIZE(pybytes));
    Py_DECREF(pybytes);
  }
  return _FileOutStream_write_cppstring(self, s);
#else
  const char* buf;
  Py_ssize_t len;
  if (!PyArg_ParseTuple(args, "s#", &buf,  &len)) {
    return NULL;
  }
  return _FileOutStream_write_cppstring(self, std::string(buf, len));
#endif
}


static PyObject *
FileOutStream_writeBytes(FileOutStreamObj *self, PyObject *args) {
  _ASSERT_STREAM_OPEN;
#if PY_MAJOR_VERSION < 3
  // "y#" not available
  PyObject *pyval, *rval;
  Py_buffer buffer = {NULL, NULL};
  if (!PyArg_ParseTuple(args, "O", &pyval)) {
    return NULL;
  }
  if (PyObject_GetBuffer(pyval, &buffer, PyBUF_SIMPLE) < 0) {
    return NULL;
  }
  std::string s((const char*)buffer.buf, buffer.len);
  rval = _FileOutStream_write_cppstring(self, s);
  PyBuffer_Release(&buffer);
  return rval;
#else
  const char* buf;
  Py_ssize_t len;
  if (!PyArg_ParseTuple(args, "y#", &buf,  &len)) {
    return NULL;
  }
  return _FileOutStream_write_cppstring(self, std::string(buf, len));
#endif
}


static PyObject *
FileOutStream_writeTuple(FileOutStreamObj *self, PyObject *args) {
  PyObject *inarg, *iterator, *item;
  char *fmt;
  _ASSERT_STREAM_OPEN;
  if (!PyArg_ParseTuple(args, "sO", &fmt, &inarg)) {
    return NULL;
  }
  if (!(iterator = PyObject_GetIter(inarg))) {
    return NULL;
  }
  for (std::size_t i = 0; i < strlen(fmt); ++i) {
    if (!(item = PyIter_Next(iterator))) {
      if (!PyErr_Occurred()) {
	PyErr_SetString(PyExc_ValueError, "not enough items");
      }
      Py_DECREF(iterator);
      return NULL;
    }
    switch(fmt[i]) {
    case 'i':
      if (!FileOutStream_writeVInt(self, PyTuple_Pack(1, item))) goto error;
      break;
    case 'l':
      if (!FileOutStream_writeVLong(self, PyTuple_Pack(1, item))) goto error;
      break;
    case 'f':
      if (!FileOutStream_writeFloat(self, PyTuple_Pack(1, item))) goto error;
      break;
    case 's':
      if (!FileOutStream_writeString(self, PyTuple_Pack(1, item))) goto error;
      break;
    case 'b':
      if (!FileOutStream_writeBytes(self, PyTuple_Pack(1, item))) goto error;
      break;
    default:
      PyErr_Format(PyExc_ValueError, "Unknown format '%c'", fmt[i]);
      goto error;
    }
    Py_DECREF(item);
  }
  Py_DECREF(iterator);
  Py_RETURN_NONE;

error:
  Py_DECREF(item);
  Py_DECREF(iterator);
  return NULL;
}


// Same as write_tuple("ibb", (OUTPUT, k, v)) or, when part is specified,
// write_tuple("iibb", (PARTITIONED_OUTPUT, part, k, v)), but more efficient.
// Optimizing other commands in this way is probably worthless.
static PyObject *
FileOutStream_writeOutput(FileOutStreamObj *self, PyObject *args) {
  int part = -1;
  PyThreadState *state;
  _ASSERT_STREAM_OPEN;
#if PY_MAJOR_VERSION < 3
  // "y#" not available
  PyObject *pykey, *pyval;
  Py_buffer kbuf = {NULL, NULL};
  Py_buffer vbuf = {NULL, NULL};
  if (!PyArg_ParseTuple(args, "OO|i", &pykey, &pyval, &part)) {
    return NULL;
  }
  if (PyObject_GetBuffer(pykey, &kbuf, PyBUF_SIMPLE) < 0) {
    return NULL;
  }
  if (PyObject_GetBuffer(pyval, &vbuf, PyBUF_SIMPLE) < 0) {
    PyBuffer_Release(&kbuf);
    return NULL;
  }
  std::string ks((const char*)kbuf.buf, kbuf.len);
  std::string vs((const char*)vbuf.buf, vbuf.len);
#else
  const char *key, *val;
  Py_ssize_t klen, vlen;
  if (!PyArg_ParseTuple(args, "y#y#|i", &key, &klen, &val, &vlen, &part)) {
    return NULL;
  }
  std::string ks(key, klen);
  std::string vs(val, vlen);
#endif
  state = PyEval_SaveThread();
  try {
    if (part >= 0) {
      HadoopUtils::serializeInt(PARTITIONED_OUTPUT, *self->stream);
      HadoopUtils::serializeInt(part, *self->stream);
    } else {
      HadoopUtils::serializeInt(OUTPUT, *self->stream);
    }
    HadoopUtils::serializeString(ks, *self->stream);
    HadoopUtils::serializeString(vs, *self->stream);
  } catch (HadoopUtils::Error e) {
    PyEval_RestoreThread(state);
    PyErr_SetString(PyExc_IOError, e.getMessage().c_str());
#if PY_MAJOR_VERSION < 3
    PyBuffer_Release(&kbuf);
    PyBuffer_Release(&vbuf);
#endif
    return NULL;
  }
  PyEval_RestoreThread(state);
#if PY_MAJOR_VERSION < 3
  PyBuffer_Release(&kbuf);
  PyBuffer_Release(&vbuf);
#endif
  Py_RETURN_NONE;
}


static PyObject *
FileOutStream_advance(FileOutStreamObj *self, PyObject *args) {
  size_t len;
  PyThreadState *state;
  _ASSERT_STREAM_OPEN;
  if (!PyArg_ParseTuple(args, "n", &len)) {
    return NULL;
  }
  state = PyEval_SaveThread();
  bool res = self->stream->advance(len);
  if (!res) {
    PyEval_RestoreThread(state);
    return PyErr_SetFromErrno(PyExc_IOError);
  }
  PyEval_RestoreThread(state);
  Py_RETURN_NONE;
}


static PyObject *
FileOutStream_flush(FileOutStreamObj *self) {
  PyThreadState *state;
  _ASSERT_STREAM_OPEN;
  state = PyEval_SaveThread();
  self->stream->flush();
  PyEval_RestoreThread(state);
  Py_RETURN_NONE;
}


static PyMethodDef FileOutStream_methods[] = {
  {"close", (PyCFunction)FileOutStream_close, METH_NOARGS,
   "close(): close the currently open file"},
  {"write", (PyCFunction)FileOutStream_write, METH_VARARGS,
   "write(data): write data to the stream"},
  {"write_vint", (PyCFunction)FileOutStream_writeVInt, METH_VARARGS,
   "write_vint(n): write a variable length integer to the stream"},
  {"write_vlong", (PyCFunction)FileOutStream_writeVLong, METH_VARARGS,
   "write_vlong(n): write a variable length long integer to the stream"},
  {"write_float", (PyCFunction)FileOutStream_writeFloat, METH_VARARGS,
   "write_float(n): write a float to the stream"},
  {"write_string", (PyCFunction)FileOutStream_writeString, METH_VARARGS,
   "write_string(n): write a string to the stream"},
  {"write_bytes", (PyCFunction)FileOutStream_writeBytes, METH_VARARGS,
   "write_bytes(n): write a bytes object to the stream"},
  {"write_tuple", (PyCFunction)FileOutStream_writeTuple, METH_VARARGS,
   "write_tuple(fmt, t): write values from iterable t according to fmt"},
  {"write_output", (PyCFunction)FileOutStream_writeOutput, METH_VARARGS,
   "write_output(key, value[, part]): write pipes [partitioned] output"},
  {"advance", (PyCFunction)FileOutStream_advance, METH_VARARGS,
   "advance(len): advance len bytes"},
  {"flush", (PyCFunction)FileOutStream_flush, METH_NOARGS,
   "flush(): flush the stream"},
  {"__enter__", (PyCFunction)FileOutStream_enter, METH_NOARGS},
  {"__exit__", (PyCFunction)FileOutStream_exit, METH_VARARGS},  
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
