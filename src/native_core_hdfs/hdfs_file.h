#ifndef PYTHON_HDFS_FILE_TYPE
#define PYTHON_HDFS_FILE_TYPE

#include <Python.h>
#include <string>
#include <map>
#include <utility>  // std::pair support
#include <iostream>
#include <errno.h>
#include <typeinfo>

#include <hdfs.h>

#include "structmember.h"


typedef struct {
    PyObject_HEAD
    hdfsFS fs;
    hdfsFile file;
    // LP: do we need this? const char* path;
    // If so, we should try to convert it to a PyObject* and use reference counting
    int flags;
    int buff_size;
    short replication;
    int blocksize;
    int readline_chunk_size;

#ifdef HADOOP_LIBHDFS_V1
    int stream_type;
#endif

} FileInfo;



PyObject* FileClass_new(PyTypeObject *type, PyObject *args, PyObject *kwds);

void FileClass_dealloc(FileInfo* self);

int FileClass_init(FileInfo *self, PyObject *args, PyObject *kwds);

int FileClass_init_internal(FileInfo *self, hdfsFS fs, hdfsFile file);

PyObject* FileClass_close(FileInfo* self);

PyObject* FileClass_mode(FileInfo* self);

PyObject* FileClass_write(FileInfo* self, PyObject *args, PyObject *kwds);

PyObject* FileClass_write_chunk(FileInfo* self, PyObject *args, PyObject *kwds);

PyObject* FileClass_get_mode(FileInfo *self);

PyObject* FileClass_available(FileInfo *self);

PyObject* FileClass_read(FileInfo *self, PyObject *args, PyObject *kwds);

PyObject* FileClass_read_chunk(FileInfo *self, PyObject *args, PyObject *kwds);

PyObject* FileClass_pread(FileInfo *self, PyObject *args, PyObject *kwds);

PyObject* FileClass_pread_chunk(FileInfo *self, PyObject *args, PyObject *kwds);

PyObject* FileClass_seek(FileInfo *self, PyObject *args, PyObject *kwds);

PyObject* FileClass_tell(FileInfo *self, PyObject *args, PyObject *kwds);

PyObject* FileClass_flush(FileInfo *self);



static PyMemberDef FileClass_members[] = {
        /*{"first", T_OBJECT_EX, offsetof(FileInfo, first), 0,
                "first name"},
        {"last", T_OBJECT_EX, offsetof(FileInfo, last), 0,
                "last name"},
        {"number", T_INT, offsetof(FileInfo, number), 0,
                "noddy number"},*/
        {NULL}  /* Sentinel */
};




static PyMethodDef FileClass_methods[] = {

        {"close", (PyCFunction)FileClass_close, METH_NOARGS,
                "FIXME"
        },


        {"available", (PyCFunction) FileClass_available, METH_NOARGS,
                "FIXME"         
        },


        {"mode", (PyCFunction)FileClass_mode, METH_NOARGS,
                "FIXME"                  
        },

        {"get_mode", (PyCFunction) FileClass_get_mode, METH_NOARGS,
                "Get the current working directory for the given filesystem."
        },

        {"write", (PyCFunction)FileClass_write, METH_VARARGS,
                "FIXME"                           
        },

        {"write_chunk", (PyCFunction)FileClass_write_chunk, METH_VARARGS,
                "FIXME"                           
        },

        {"flush", (PyCFunction) FileClass_flush, METH_NOARGS,
                "FIXME"                                    
        },

        {"read", (PyCFunction) FileClass_read, METH_VARARGS,
                "FIXME"                                    
        },

        {"read_chunk", (PyCFunction) FileClass_read_chunk, METH_VARARGS,
                "FIXME"                                    
        },

        {"pread", (PyCFunction) FileClass_pread, METH_VARARGS,
                "FIXME"                                    
        },

        {"pread_chunk", (PyCFunction) FileClass_pread_chunk, METH_VARARGS,
                "FIXME"                                    
        },

        {"seek", (PyCFunction) FileClass_seek, METH_VARARGS,
                "FIXME"                                   
        },

        {"tell", (PyCFunction) FileClass_tell, METH_NOARGS,
                "FIXME"                                   
        },

        {NULL}  /* Sentinel */
};





static PyTypeObject FileType = {
        PyObject_HEAD_INIT(NULL)
        0,                         /*ob_size*/
        "native_core_hdfs.CoreHdfsFile",  /*tp_name*/
        sizeof(FileInfo),          /*tp_basicsize*/
        0,                         /*tp_itemsize*/
        (destructor)FileClass_dealloc, /*tp_dealloc*/
        0,                         /*tp_print*/
        0,                         /*tp_getattr*/
        0,                         /*tp_setattr*/
        0,                         /*tp_compare*/
        0,                         /*tp_repr*/
        0,                         /*tp_as_number*/
        0,                         /*tp_as_sequence*/
        0,                         /*tp_as_mapping*/
        0,                         /*tp_hash */
        0,                         /*tp_call*/
        0,                         /*tp_str*/
        0,                         /*tp_getattro*/
        0,                         /*tp_setattro*/
        0,                         /*tp_as_buffer*/
        Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE, /*tp_flags*/
        "Hdfs File objects",       /* tp_doc */
        0,                         /* tp_traverse */
        0,                         /* tp_clear */
        0,                         /* tp_richcompare */
        0,                         /* tp_weaklistoffset */
        0,                         /* tp_iter */
        0,                         /* tp_iternext */
        FileClass_methods,         /* tp_methods */
        FileClass_members,         /* tp_members */
        0,                         /* tp_getset */
        0,                         /* tp_base */
        0,                         /* tp_dict */
        0,                         /* tp_descr_get */
        0,                         /* tp_descr_set */
        0,                         /* tp_dictoffset */
        (initproc)FileClass_init,  /* tp_init */
        0,                         /* tp_alloc */
        FileClass_new,             /* tp_new */
};

#endif
