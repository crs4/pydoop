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

#if PY_MAJOR_VERSION >= 3
#define IS_PY3K 1
#endif

#include "hdfs_fs.h"
#include "hdfs_file.h"
#include <jni.h>

static char* module__name__ = "native_core_hdfs";
static char* module__doc__ = "native_hdfs_core implementation";

/* FsType */
static PyMemberDef FsClass_members[] = {
  {NULL}  /* Sentinel */
};

static PyMethodDef FsClass_methods[] = {
  {"get_working_directory", (PyCFunction) FsClass_get_working_directory,
   METH_NOARGS, "Get the current working directory"},
  {"get_path_info", (PyCFunction) FsClass_get_path_info, METH_VARARGS,
   "Get information on a file or directory"},
  {"get_default_block_size", (PyCFunction) FsClass_get_default_block_size,
   METH_NOARGS, "Get the default block size"},
  {"get_hosts", (PyCFunction) FsClass_get_hosts, METH_VARARGS,
   "Get the names of the hosts where a file is stored"},
  {"get_capacity", (PyCFunction) FsClass_get_capacity, METH_VARARGS,
   "Get the raw capacity of the filesystem"},
  {"get_used", (PyCFunction) FsClass_get_used, METH_NOARGS,
   "Get the total raw size of all files in the filesystem."},
  {"set_replication", (PyCFunction) FsClass_set_replication, METH_VARARGS,
   "Set the replication factor for a file"},
  {"set_working_directory", (PyCFunction) FsClass_set_working_directory,
   METH_VARARGS, "Set the current working directory"},
  {"open_file", (PyCFunction) FsClass_open_file, METH_VARARGS, "Open a file"},
  {"close", (PyCFunction) FsClass_close, METH_NOARGS,
   "Close the HDFS connection"},
  {"copy", (PyCFunction) FsClass_copy, METH_VARARGS, "Copy the given file"},
  {"create_directory", (PyCFunction) FsClass_create_directory, METH_VARARGS,
   "Create a directory with the given name"},
  {"list_directory", (PyCFunction) FsClass_list_directory, METH_VARARGS,
   "Get the contents of a directory"},
  {"move", (PyCFunction) FsClass_move, METH_VARARGS, "Move the given file"},
  {"rename", (PyCFunction) FsClass_rename, METH_VARARGS,
   "Rename the given file"},
  {"delete", (PyCFunction) FsClass_delete, METH_VARARGS,
   "Delete the given file or directory"},
  {"exists", (PyCFunction) FsClass_exists, METH_VARARGS,
   "Check if the given path exists on the filesystem"},
  {"chmod", (PyCFunction) FsClass_chmod, METH_VARARGS, "Change file mode"},
  {"chown", (PyCFunction) FsClass_chown, METH_VARARGS,
   "Change file owner and group"},
  {"utime", (PyCFunction) FsClass_utime, METH_VARARGS,
   "Change file last access and modification time"},
  {NULL}  /* Sentinel */
};

static PyTypeObject FsType = {
  PyVarObject_HEAD_INIT(NULL, 0)  
  "native_core_hdfs.CoreHdfsFs",            /* tp_name */
  sizeof(FsInfo),                           /* tp_basicsize */
  0,                                        /* tp_itemsize */
  (destructor) FsClass_dealloc,             /* tp_dealloc */
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
  "Hdfs FS objects",                        /* tp_doc */
  0,                                        /* tp_traverse */
  0,                                        /* tp_clear */
  0,                                        /* tp_richcompare */
  0,                                        /* tp_weaklistoffset */
  0,                                        /* tp_iter */
  0,                                        /* tp_iternext */
  FsClass_methods,                          /* tp_methods */
  FsClass_members,                          /* tp_members */
  0,                                        /* tp_getset */
  0,                                        /* tp_base */
  0,                                        /* tp_dict */
  0,                                        /* tp_descr_get */
  0,                                        /* tp_descr_set */
  0,                                        /* tp_dictoffset */
  (initproc) FsClass_init,                  /* tp_init */
  0,                                        /* tp_alloc */
  FsClass_new,                              /* tp_new */
};


/* FileType */
static PyMemberDef FileClass_members[] = {
  {NULL}  /* Sentinel */
};

static PyGetSetDef FileClass_getseters[] = {
  {"closed", (getter)FileClass_getclosed, NULL, NULL},
  {"buff_size", (getter)FileClass_getbuff_size, NULL, NULL},
  {"name", (getter)FileClass_getname, NULL, NULL},
  {"mode", (getter)FileClass_getmode, NULL, NULL},
  {NULL}  /* Sentinel */
};

static PyMethodDef FileClass_methods[] = {
  {"close", (PyCFunction)FileClass_close, METH_NOARGS, "Close the file"},
  {"readable", (PyCFunction)FileClass_readable, METH_NOARGS,
   "True if the file can be read from"},
  {"writable", (PyCFunction)FileClass_writable, METH_NOARGS,
   "True if the file can be written to"},
  {"seekable", (PyCFunction)FileClass_seekable, METH_NOARGS,
   "True if the file support random access (it does if it's readable)"},
  {"available", (PyCFunction) FileClass_available, METH_NOARGS,
   "Number of bytes that can be read without blocking"},
  {"write", (PyCFunction)FileClass_write, METH_VARARGS, "Write to the file"},
  {"flush", (PyCFunction) FileClass_flush, METH_NOARGS,
   "Force any buffered output to be written"},
  {"read", (PyCFunction) FileClass_read, METH_VARARGS, "Read from the file"},
  {"read_chunk", (PyCFunction) FileClass_read_chunk, METH_VARARGS,
   "Like read, but store data to the given buffer"},
  /* Also export read_chunk as readinto for compatibility with Python io */
  {"readinto", (PyCFunction) FileClass_read_chunk, METH_VARARGS,
   "Like read, but store data to the given buffer"},
  {"pread", (PyCFunction) FileClass_pread, METH_VARARGS,
   "Read starting from the given position"},
  {"pread_chunk", (PyCFunction) FileClass_pread_chunk, METH_VARARGS,
   "Like pread, but store data to the given buffer"},
  {"seek", (PyCFunction) FileClass_seek, METH_VARARGS,
   "Seek to the given position"},
  {"tell", (PyCFunction) FileClass_tell, METH_NOARGS,
   "Get the current position"},
  {NULL}  /* Sentinel */
};

static PyTypeObject FileType = {
  PyVarObject_HEAD_INIT(NULL, 0)  
  "native_core_hdfs.CoreHdfsFile",          /* tp_name */
  sizeof(FileInfo),                         /* tp_basicsize */
  0,                                        /* tp_itemsize */
  (destructor)FileClass_dealloc,            /* tp_dealloc */
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
  "Hdfs File objects",                      /* tp_doc */
  0,                                        /* tp_traverse */
  0,                                        /* tp_clear */
  0,                                        /* tp_richcompare */
  0,                                        /* tp_weaklistoffset */
  0,                                        /* tp_iter */
  0,                                        /* tp_iternext */
  FileClass_methods,                        /* tp_methods */
  FileClass_members,                        /* tp_members */
  FileClass_getseters,                      /* tp_getset */
  0,                                        /* tp_base */
  0,                                        /* tp_dict */
  0,                                        /* tp_descr_get */
  0,                                        /* tp_descr_set */
  0,                                        /* tp_dictoffset */
  (initproc)FileClass_init,                 /* tp_init */
  0,                                        /* tp_alloc */
  FileClass_new,                            /* tp_new */
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
PyInit_native_core_hdfs(void)
{
  PyObject* m;

  if (PyType_Ready(&FsType) < 0)
    return NULL;
  if (PyType_Ready(&FileType) < 0)
    return NULL;
  m = PyModule_Create(&module_def);
  if (m == NULL)
    return NULL;

  Py_INCREF(&FsType);
  Py_INCREF(&FileType);
  PyModule_AddObject(m, "CoreHdfsFs", (PyObject *)&FsType);
  PyModule_AddObject(m, "CoreHdfsFile", (PyObject *)&FileType);

  return m;
}

#else

PyMODINIT_FUNC
initnative_core_hdfs(void)
{
  PyObject* m;

  if (PyType_Ready(&FsType) < 0)
    return;
  if (PyType_Ready(&FileType) < 0)
    return;
  m = Py_InitModule3(module__name__, module_methods,
                     module__doc__);
  if (m == NULL)
    return;

  Py_INCREF(&FsType);
  Py_INCREF(&FileType);
  PyModule_AddObject(m, "CoreHdfsFs", (PyObject *)&FsType);
  PyModule_AddObject(m, "CoreHdfsFile", (PyObject *)&FileType);

  PyModule_AddStringConstant(m, "MODE_READ", MODE_READ);
  PyModule_AddStringConstant(m, "MODE_WRITE", MODE_WRITE);
  PyModule_AddStringConstant(m, "MODE_APPEND", MODE_APPEND);
}
#endif

