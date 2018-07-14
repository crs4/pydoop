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

#include "streams.h"

const char* m_name = "sercore";
const char* m_doc = "core serialization utils";

#if PY_MAJOR_VERSION >= 3
#define PY3
#define INIT_RETURN(V) return V;
#else
#define INIT_RETURN(V) return;
#endif


static PyMethodDef SercoreMethods[] = {
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
