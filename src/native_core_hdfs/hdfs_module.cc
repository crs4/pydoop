/* BEGIN_COPYRIGHT
 *
 * Copyright 2009-2014 CRS4.
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
            "native_hdfs_core implementation");

    if (m == NULL)
        return;

    Py_INCREF(&FsType);
    Py_INCREF(&FileType);


    PyModule_AddObject(m, "CoreHdfsFs", (PyObject *)&FsType);
    PyModule_AddObject(m, "CoreHdfsFile", (PyObject *)&FileType);
}


