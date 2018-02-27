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
#include <Python.h>
#include <structmember.h>


#if PY_MAJOR_VERSION >= 3
#define IS_PY3K 1
#define Py_TPFLAGS_HAVE_ITER 0
#endif

#include "flow.hh"
#include "command.hh"


static char* module__name__ = "sercore";
static char* module__doc__ =  "serialization low level implementation";


/* CommandWriter */
static PyMemberDef CommandWriter_members[] = {
  {NULL}  /* Sentinel */
};

static PyMethodDef CommandWriter_methods[] = {
  {"write", (PyCFunction) CommandWriter_write, METH_O,
   "Write (cmd_code, args) as a command."},
  {"flush", (PyCFunction) CommandWriter_flush, METH_NOARGS,
   "flush the attached output stream."},
  {"close", (PyCFunction) CommandWriter_close, METH_NOARGS,
   "close the attached output stream."},
  {NULL, NULL, 0, NULL}        /* Sentinel */
};


static PyTypeObject CommandWriterType = {
  PyVarObject_HEAD_INIT(NULL, 0)  
  "sercore.CommandWriter",                  /* tp_name */
  sizeof(CommandWriterInfo),                /* tp_basicsize */
  0,                                        /* tp_itemsize */
  (destructor) CommandWriter_dealloc,       /* tp_dealloc */
  0,                                        /* tp_print */
  0,                                        /* tp_getattr */
  0,                                        /* tp_setattr */
  0,                                        /* tp_compare */
  0,                                        /* tp_repr */
  0,                                        /* tp_as_number */
  0,                                        /* tp_as_sequence */
  0,                                        /* tp_as_mapping */
  0,                                        /* tp_hash */
  0,                                        /* tp_call */
  0,                                        /* tp_str */
  0,                                        /* tp_getattro */
  0,                                        /* tp_setattro */
  0,                                        /* tp_as_buffer */
  Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE, /* tp_flags */
  "CommandWriter objects",                 /* tp_doc */
  0,                                        /* tp_traverse */
  0,                                        /* tp_clear */
  0,                                        /* tp_richcompare */
  0,                                        /* tp_weaklistoffset */
  0,                                        /* tp_iter */
  0,                                        /* tp_iternext */
  CommandWriter_methods,                   /* tp_methods */
  CommandWriter_members,                   /* tp_members */
  0,                                        /* tp_getset */
  0,                                        /* tp_base */
  0,                                        /* tp_dict */
  0,                                        /* tp_descr_get */
  0,                                        /* tp_descr_set */
  0,                                        /* tp_dictoffset */
  (initproc) CommandWriter_init,           /* tp_init */
  0,                                        /* tp_alloc */
  CommandWriter_new,                       /* tp_new */
};


/* CommandReader */
static PyMemberDef CommandReader_members[] = {
  {NULL}  /* Sentinel */
};

static PyMethodDef CommandReader_methods[] = {
  {"read", (PyCFunction) CommandReader_read, METH_NOARGS,
   "Read a command."},
  {"close", (PyCFunction) CommandReader_close, METH_NOARGS,
   "close the attached input stream."},
  {NULL, NULL, 0, NULL}        /* Sentinel */
};


static PyTypeObject CommandReaderType = {
  PyVarObject_HEAD_INIT(NULL, 0)  
  "sercore.CommandReader",                  /* tp_name */
  sizeof(CommandReaderInfo),                /* tp_basicsize */
  0,                                        /* tp_itemsize */
  (destructor) CommandReader_dealloc,       /* tp_dealloc */
  0,                                        /* tp_print */
  0,                                        /* tp_getattr */
  0,                                        /* tp_setattr */
  0,                                        /* tp_compare */
  0,                                        /* tp_repr */
  0,                                        /* tp_as_number */
  0,                                        /* tp_as_sequence */
  0,                                        /* tp_as_mapping */
  0,                                        /* tp_hash */
  0,                                        /* tp_call */
  0,                                        /* tp_str */
  0,                                        /* tp_getattro */
  0,                                        /* tp_setattro */
  0,                                        /* tp_as_buffer */
  Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE | Py_TPFLAGS_HAVE_ITER, /* tp_flags */
  "CommandReader objects",                 /* tp_doc */
  0,                                        /* tp_traverse */
  0,                                        /* tp_clear */
  0,                                        /* tp_richcompare */
  0,                                        /* tp_weaklistoffset */
  CommandReader_iter,                       /* tp_iter */
  CommandReader_iternext,                   /* tp_iternext */
  CommandReader_methods,                    /* tp_methods */
  CommandReader_members,                    /* tp_members */
  0,                                        /* tp_getset */
  0,                                        /* tp_base */
  0,                                        /* tp_dict */
  0,                                        /* tp_descr_get */
  0,                                        /* tp_descr_set */
  0,                                        /* tp_dictoffset */
  (initproc) CommandReader_init,            /* tp_init */
  0,                                        /* tp_alloc */
  CommandReader_new,                        /* tp_new */
};


/* FlowWriter */
static PyMemberDef FlowWriter_members[] = {
  {NULL}  /* Sentinel */
};

static PyMethodDef FlowWriter_methods[] = {
  {"write", (PyCFunction) FlowWriter_write, METH_O,
   "Given the tuple (rule, data) writes data following rule."},
  {"flush", (PyCFunction) FlowWriter_flush, METH_NOARGS,
   "flush the attached output stream."},
  {"close", (PyCFunction) FlowWriter_close, METH_NOARGS,
   "close the attached output stream."},
  {NULL, NULL, 0, NULL}        /* Sentinel */
};


static PyTypeObject FlowWriterType = {
  PyVarObject_HEAD_INIT(NULL, 0)  
  "sercore.FlowWriter",                  /* tp_name */
  sizeof(FlowWriterInfo),                /* tp_basicsize */
  0,                                        /* tp_itemsize */
  (destructor) FlowWriter_dealloc,       /* tp_dealloc */
  0,                                        /* tp_print */
  0,                                        /* tp_getattr */
  0,                                        /* tp_setattr */
  0,                                        /* tp_compare */
  0,                                        /* tp_repr */
  0,                                        /* tp_as_number */
  0,                                        /* tp_as_sequence */
  0,                                        /* tp_as_mapping */
  0,                                        /* tp_hash */
  0,                                        /* tp_call */
  0,                                        /* tp_str */
  0,                                        /* tp_getattro */
  0,                                        /* tp_setattro */
  0,                                        /* tp_as_buffer */
  Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE, /* tp_flags */
  "FlowWriter objects",                     /* tp_doc */
  0,                                        /* tp_traverse */
  0,                                        /* tp_clear */
  0,                                        /* tp_richcompare */
  0,                                        /* tp_weaklistoffset */
  0,                                        /* tp_iter */
  0,                                        /* tp_iternext */
  FlowWriter_methods,                       /* tp_methods */
  FlowWriter_members,                       /* tp_members */
  0,                                        /* tp_getset */
  0,                                        /* tp_base */
  0,                                        /* tp_dict */
  0,                                        /* tp_descr_get */
  0,                                        /* tp_descr_set */
  0,                                        /* tp_dictoffset */
  (initproc) FlowWriter_init,           /* tp_init */
  0,                                        /* tp_alloc */
  FlowWriter_new,                       /* tp_new */
};


/* FlowReader */
static PyMemberDef FlowReader_members[] = {
  {NULL}  /* Sentinel */
};

static PyMethodDef FlowReader_methods[] = {
  {"read", (PyCFunction) FlowReader_read, METH_O,
   "Read from flow as per rule."},
  {"skip", (PyCFunction) FlowReader_skip, METH_O,
   "skip forward n bytes."},
  {"close", (PyCFunction) FlowReader_close, METH_NOARGS,
   "close the attached input stream."},
  {NULL, NULL, 0, NULL}        /* Sentinel */
};


static PyTypeObject FlowReaderType = {
  PyVarObject_HEAD_INIT(NULL, 0)  
  "sercore.FlowReader",                  /* tp_name */
  sizeof(FlowReaderInfo),                /* tp_basicsize */
  0,                                        /* tp_itemsize */
  (destructor) FlowReader_dealloc,       /* tp_dealloc */
  0,                                        /* tp_print */
  0,                                        /* tp_getattr */
  0,                                        /* tp_setattr */
  0,                                        /* tp_compare */
  0,                                        /* tp_repr */
  0,                                        /* tp_as_number */
  0,                                        /* tp_as_sequence */
  0,                                        /* tp_as_mapping */
  0,                                        /* tp_hash */
  0,                                        /* tp_call */
  0,                                        /* tp_str */
  0,                                        /* tp_getattro */
  0,                                        /* tp_setattro */
  0,                                        /* tp_as_buffer */
  Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE, /* tp_flags */  
  "FlowReader objects",                 /* tp_doc */
  0,                                        /* tp_traverse */
  0,                                        /* tp_clear */
  0,                                        /* tp_richcompare */
  0,                                        /* tp_weaklistoffset */
  0,                                        /* tp_iter */
  0,                                        /* tp_iternext */
  FlowReader_methods,                       /* tp_methods */
  FlowReader_members,                       /* tp_members */
  0,                                        /* tp_getset */
  0,                                        /* tp_base */
  0,                                        /* tp_dict */
  0,                                        /* tp_descr_get */
  0,                                        /* tp_descr_set */
  0,                                        /* tp_dictoffset */
  (initproc) FlowReader_init,            /* tp_init */
  0,                                        /* tp_alloc */
  FlowReader_new,                        /* tp_new */
};

static PyMethodDef module_methods[] = {
        {NULL}  /* Sentinel */
};

#ifndef PyMODINIT_FUNC	/* declarations for DLL import/export */
#define PyMODINIT_FUNC void
#endif

#if IS_PY3K
static struct PyModuleDef module_def = {
  PyModuleDef_HEAD_INIT,
  module__name__, /* m_name */
  module__doc__,  /* m_doc */
  -1,                  /* m_size */
  module_methods,    /* m_methods */
  NULL,                /* m_reload */
  NULL,                /* m_traverse */
  NULL,                /* m_clear */
  NULL,                /* m_free */
};
#endif


#if IS_PY3K

PyMODINIT_FUNC
PyInit_sercore(void)
{
  PyObject* m;
  if (PyType_Ready(&CommandReaderType) < 0) {
    return NULL;
  }
  if (PyType_Ready(&CommandWriterType) < 0) {
    return NULL;
  }
  if (PyType_Ready(&FlowReaderType) < 0) {
    return NULL;
  }
  if (PyType_Ready(&FlowWriterType) < 0) {
    return NULL;
  }
  
  m = PyModule_Create(&module_def);
  if (m == NULL)
    return NULL;
  Py_INCREF(&CommandWriterType);
  Py_INCREF(&CommandReaderType);
  PyModule_AddObject(m, "CommandWriter",
                     (PyObject *)&CommandWriterType);
  PyModule_AddObject(m, "CommandReader",
                     (PyObject *)&CommandReaderType);
  Py_INCREF(&FlowWriterType);
  Py_INCREF(&FlowReaderType);
  PyModule_AddObject(m, "FlowWriter",
                     (PyObject *)&FlowWriterType);
  PyModule_AddObject(m, "FlowReader",
                     (PyObject *)&FlowReaderType);
  PyModule_AddObject(m, "RULES", get_rules());
  return m;
}

#else

PyMODINIT_FUNC
initsercore(void)
{
  PyObject* m;

  if (PyType_Ready(&CommandWriterType) < 0)
    return;
  if (PyType_Ready(&CommandReaderType) < 0)
    return;
  if (PyType_Ready(&FlowWriterType) < 0)
    return;
  if (PyType_Ready(&FlowReaderType) < 0)
    return;
  m = Py_InitModule3(module__name__, module_methods,
                     module__doc__);
  if (m == NULL)
    return;
  
  Py_INCREF(&CommandWriterType);
  Py_INCREF(&CommandReaderType);
  PyModule_AddObject(m, "CommandWriter",
                     (PyObject *)&CommandWriterType);
  PyModule_AddObject(m, "CommandReader",
                     (PyObject *)&CommandReaderType);
  Py_INCREF(&FlowWriterType);
  Py_INCREF(&FlowReaderType);
  PyModule_AddObject(m, "FlowWriter",
                     (PyObject *)&FlowWriterType);
  PyModule_AddObject(m, "FlowReader",
                     (PyObject *)&FlowReaderType);
  PyModule_AddObject(m, "RULES", get_rules());
}

#endif
