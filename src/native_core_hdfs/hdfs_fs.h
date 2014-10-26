#ifndef PYTHON_HDFS_FS_TYPE
#define PYTHON_HDFS_FS_TYPE

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
    char *host;
    int port;
    char *user;
    char *group;
    hdfsFS _fs;
} FsInfo;


static PyMemberDef FsClass_members[] = {
        /*{"host", T_STRING_INPLACE, offsetof(FsInfo, host), 0,
                "first name"},
        {"user", T_STRING, offsetof(FsInfo, user), 0,
                "last name"},
        {"group", T_STRING, offsetof(FsInfo, group), 0,
                "last name"},
        {"port", T_INT, offsetof(FsInfo, port), 0,
                "noddy number"},
        {"_fs", T_OBJECT, offsetof(FsInfo, _fs), 0,
                "fs connection"},
                */
        {NULL}  /* Sentinel */
};


PyObject* FsClass_new(PyTypeObject* type, PyObject *args, PyObject *kwds);

void FsClass_dealloc(FsInfo* self);

int FsClass_init(FsInfo* self, PyObject *args, PyObject *kwds);

PyObject* FsClass_close(FsInfo* self);

PyObject* FsClass_name(FsInfo* self);

PyObject* FsClass_working_directory(FsInfo* self);

PyObject* FsClass_get_working_directory(FsInfo* self);

PyObject* FsClass_default_block_size(FsInfo* self);

PyObject* FsClass_get_default_block_size(FsInfo* self);

PyObject* FsClass_path_info(FsInfo* self, PyObject *args, PyObject *kwds);

PyObject* FsClass_get_path_info(FsInfo* self, PyObject *args, PyObject *kwds);

PyObject* FsClass_get_hosts(FsInfo* self, PyObject *args, PyObject *kwds);

PyObject* FsClass_used(FsInfo* self);

PyObject* FsClass_get_used(FsInfo* self);

PyObject* FsClass_capacity(FsInfo* self);

PyObject* FsClass_get_capacity(FsInfo* self);

PyObject* FsClass_set_replication(FsInfo* self, PyObject *args, PyObject *kwds);

PyObject* FsClass_set_working_directory(FsInfo* self, PyObject *args, PyObject *kwds);

PyObject* FsClass_open_file(FsInfo* self, PyObject *args, PyObject *kwds);

PyObject* FsClass_copy(FsInfo* self, PyObject *args, PyObject *kwds);

PyObject* FsClass_exists(FsInfo* self, PyObject *args, PyObject *kwds);

PyObject*FsClass_list_directory(FsInfo *self, PyObject *args, PyObject *kwds);

PyObject* FsClass_create_directory(FsInfo* self, PyObject *args, PyObject *kwds);

PyObject* FsClass_rename(FsInfo* self, PyObject *args, PyObject *kwds);

PyObject* FsClass_move(FsInfo* self, PyObject *args, PyObject *kwds);

PyObject* FsClass_delete(FsInfo* self, PyObject *args, PyObject *kwds);

PyObject* FsClass_chmod(FsInfo* self, PyObject *args, PyObject *kwds);

PyObject* FsClass_chown(FsInfo* self, PyObject *args, PyObject *kwds);

PyObject* FsClass_utime(FsInfo* self, PyObject *args, PyObject *kwds);



static PyMethodDef FsClass_methods[] = {
        {"name", (PyCFunction) FsClass_name, METH_NOARGS,
                "Return the name, combining the first and last name"
        },
        
        {"working_directory", (PyCFunction) FsClass_working_directory, METH_NOARGS,
                "Get the current working directory for the given filesystem."
        },

        {"get_working_directory", (PyCFunction) FsClass_get_working_directory, METH_NOARGS,
                "Get the current working directory for the given filesystem."
        },

        {"path_info", (PyCFunction) FsClass_path_info, METH_VARARGS,
                "Get the current working directory for the given filesystem."
        },

        {"get_path_info", (PyCFunction) FsClass_get_path_info, METH_VARARGS,
                "Get the current working directory for the given filesystem."
        },

        {"default_block_size", (PyCFunction) FsClass_default_block_size, METH_NOARGS,
                "Get the current working directory for the given filesystem."
        },

        {"get_default_block_size", (PyCFunction) FsClass_get_default_block_size, METH_NOARGS,
                "Get the current working directory for the given filesystem."
        },

        {"get_hosts", (PyCFunction) FsClass_get_hosts, METH_VARARGS,
                "Get the current working directory for the given filesystem."
        },

        {"capacity", (PyCFunction) FsClass_capacity, METH_VARARGS,
                "Get the current working directory for the given filesystem."
        },

        {"get_capacity", (PyCFunction) FsClass_get_capacity, METH_VARARGS,
                "Get the current working directory for the given filesystem."
        },

        {"used", (PyCFunction) FsClass_used, METH_NOARGS,
                "Get the current working directory for the given filesystem."
        },

        {"get_used", (PyCFunction) FsClass_get_used, METH_NOARGS,
                "Get the current working directory for the given filesystem."
        },


        {"set_replication", (PyCFunction) FsClass_set_replication, METH_VARARGS,
                "Get the current working directory for the given filesystem."
        },

        {"set_working_directory", (PyCFunction) FsClass_set_working_directory, METH_VARARGS,
                "Get the current working directory for the given filesystem."
        },

        {"open_file", (PyCFunction) FsClass_open_file, METH_VARARGS,
                "Open a file"
        },

        {"close", (PyCFunction) FsClass_close, METH_NOARGS,
                "Close the connection to the HDFS"
        },

        {"copy", (PyCFunction) FsClass_copy, METH_VARARGS,
                "Get the current working directory for the given filesystem."
        },

        {"create_directory", (PyCFunction) FsClass_create_directory, METH_VARARGS,
                "Get the current working directory for the given filesystem."
        },

        {"list_directory", (PyCFunction) FsClass_list_directory, METH_VARARGS,
                "Get the current working directory for the given filesystem."
        },

        {"move", (PyCFunction) FsClass_move, METH_VARARGS,
                "Get the current working directory for the given filesystem."
        },

        {"rename", (PyCFunction) FsClass_rename, METH_VARARGS,
                "Get the current working directory for the given filesystem."
        },

        {"delete", (PyCFunction) FsClass_delete, METH_VARARGS,
                "Get the current working directory for the given filesystem."
        },

        {"exists", (PyCFunction) FsClass_exists, METH_VARARGS,
                "Get the current working directory for the given filesystem."
        },

        {"chmod", (PyCFunction) FsClass_chmod, METH_VARARGS,
            "Get the current working directory for the given filesystem."
        },

        {"chown", (PyCFunction) FsClass_chown, METH_VARARGS,
                "Get the current working directory for the given filesystem."
        },

        {"utime", (PyCFunction) FsClass_utime, METH_VARARGS,
                "Get the current working directory for the given filesystem."
        },

        {NULL}  /* Sentinel */
};


static PyTypeObject FsType = {
        PyObject_HEAD_INIT(NULL)
        0,                         /*ob_size*/
        "native_core_hdfs.CoreHdfsFs",             /*tp_name*/
        sizeof(FsInfo),             /*tp_basicsize*/
        0,                         /*tp_itemsize*/
        (destructor) FsClass_dealloc, /*tp_dealloc*/
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
        "Hdfs FS objects",           /* tp_doc */
        0,                       /* tp_traverse */
        0,                       /* tp_clear */
        0,                       /* tp_richcompare */
        0,                       /* tp_weaklistoffset */
        0,                       /* tp_iter */
        0,                       /* tp_iternext */
        FsClass_methods,             /* tp_methods */
        FsClass_members,             /* tp_members */
        0,                         /* tp_getset */
        0,                         /* tp_base */
        0,                         /* tp_dict */
        0,                         /* tp_descr_get */
        0,                         /* tp_descr_set */
        0,                         /* tp_dictoffset */
        (initproc) FsClass_init,      /* tp_init */
        0,                         /* tp_alloc */
        FsClass_new,                 /* tp_new */
};

#endif
