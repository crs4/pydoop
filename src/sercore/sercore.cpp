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

#include <Python.h>

#include "hu_extras.h"
#include "streams.h"

const char* m_name = "sercore";
const char* m_doc = "core serialization utils";

#if PY_MAJOR_VERSION >= 3
#define PY3
#define INIT_RETURN(V) return V;
#else
#define INIT_RETURN(V) return;
#endif


// Deserializes a hadoop.(mapred|mapreduce.lib.input).FileSplit
static PyObject *
deserializeFileSplit(PyObject *self, PyObject *args) {
  PyObject *data, *rval;
  Py_buffer buffer = {NULL, NULL};
  PyThreadState *state;
  if (!PyArg_ParseTuple(args, "O", &data)) {
    return NULL;
  }
  if (PyObject_GetBuffer(data, &buffer, PyBUF_SIMPLE) < 0) {
    PyErr_SetString(PyExc_TypeError, "data not accessible as a buffer");
    return NULL;
  }

  // deserialize fields
  std::string s((const char*)buffer.buf, buffer.len);
  HadoopUtils::StringInStream stream(s);
  std::string fname;
  int64_t offset, length;
  state = PyEval_SaveThread();
  try {
    HadoopUtils::deserializeString(fname, stream);
    offset = deserializeLongWritable(stream);
    length = deserializeLongWritable(stream);
  } catch (HadoopUtils::Error e) {
    PyEval_RestoreThread(state);
    PyBuffer_Release(&buffer);
    PyErr_SetString(PyExc_IOError, e.getMessage().c_str());
    return NULL;
  }
  PyEval_RestoreThread(state);
  PyBuffer_Release(&buffer);

  // build output tuple
  PyObject *_fname, *_offset, *_length;
  if (!(_fname = PyUnicode_FromStringAndSize(fname.c_str(), fname.size()))) {
    return NULL;
  }
  if (!(_offset = Py_BuildValue("L", offset))) {
    return NULL;
  }
  if (!(_length = Py_BuildValue("L", length))) {
    return NULL;
  }
  if (!(rval = PyTuple_New(3))) {
    return NULL;
  }
  PyTuple_SET_ITEM(rval, 0, _fname);
  PyTuple_SET_ITEM(rval, 1, _offset);
  PyTuple_SET_ITEM(rval, 2, _length);
  return rval;
}


static PyMethodDef SercoreMethods[] = {
  {"deserialize_file_split", deserializeFileSplit, METH_VARARGS,
   "deserialize_file_split(data): deserialize a Hadoop FileSplit"},
  {NULL}
};


#ifdef PY3
static struct PyModuleDef module_def = {
  PyModuleDef_HEAD_INIT,
  m_name,
  m_doc,
  0,
  SercoreMethods,
  NULL,
  NULL,
  NULL,
  NULL
};
#endif


PyMODINIT_FUNC
#ifdef PY3
PyInit_sercore(void) {
#else
initsercore(void) {
#endif
  PyObject *m;
  FileInStreamType.tp_new = PyType_GenericNew;
  if (PyType_Ready(&FileInStreamType) < 0) {
    INIT_RETURN(NULL);;
  }
  FileOutStreamType.tp_new = PyType_GenericNew;
  if (PyType_Ready(&FileOutStreamType) < 0) {
    INIT_RETURN(NULL);;
  }
#ifdef PY3
  m = PyModule_Create(&module_def);
#else
  m = Py_InitModule3(m_name, SercoreMethods, m_doc);
#endif
  if (!m) {
    INIT_RETURN(NULL);;
  }
  Py_INCREF(&FileInStreamType);
  PyModule_AddObject(m, "FileInStream", (PyObject *)&FileInStreamType);
  Py_INCREF(&FileOutStreamType);
  PyModule_AddObject(m, "FileOutStream", (PyObject *)&FileOutStreamType);
  INIT_RETURN(m);
}
