#include <Python.h>

#include "hdfs_fs.h"
#include "hdfs_file.h"
#include <jni.h>


static PyMethodDef module_methods[] = {
        {NULL}  /* Sentinel */
};


#ifndef PyMODINIT_FUNC	/* declarations for DLL import/export */
#define PyMODINIT_FUNC void
#endif



PyMODINIT_FUNC
initnative_core_hdfs(void)
{
    PyObject* m;

    if (PyType_Ready(&FsType) < 0)
        return;

    if (PyType_Ready(&FileType) < 0)
        return;

    m = Py_InitModule3("native_core_hdfs", module_methods,
            "Example module that creates an extension type.");

    if (m == NULL)
        return;

    Py_INCREF(&FsType);
    Py_INCREF(&FileType);


    PyModule_AddObject(m, "CoreHdfsFs", (PyObject *)&FsType);
    PyModule_AddObject(m, "CoreHdfsFile", (PyObject *)&FileType);


    /*
    // Load the module object
    PyObject* pModule = PyImport_ImportModule("pydoop.hdfs.hadoop.hdfs");

    // pDict is a borrowed reference
    PyObject*  pDict = PyModule_GetDict(pModule);

    // pFunc is also a borrowed reference
    PyObject* pFunc = PyDict_GetItemString(pDict, "FileSystem.register");
    PyObject* args = Py_BuildValue("O",(PyObject *)&FsType);


    if (PyCallable_Check(pFunc))
    {
        PyObject_CallObject(pFunc, args);
    } else
    {
        PyErr_Print();
    }

    // Clean up
    Py_DECREF(pModule);
    */
}


