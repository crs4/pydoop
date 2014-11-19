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

#include "hdfs_utils.h"
#include <iostream>
#include <stdarg.h>
#include <unicodeobject.h>

using namespace hdfs4python;

char* Utils::getObjectAsUTF8String(PyObject *obj) {

    PyObject *utfString = obj;

    if(PyUnicode_Check(obj)) {
       utfString = PyUnicode_AsUTF8String(obj);
    }

    Py_buffer pybuf;
    PyObject_GetBuffer(utfString, &pybuf, PyBUF_SIMPLE);

    if(pybuf.buf==NULL)
        return NULL;

    PyBuffer_Release(&pybuf);


    return (char*) pybuf.buf;
}


char* Utils::getUnicodeAsUTF8String(PyUnicodeObject *obj) {

    if(obj==NULL)
        return NULL;

    PyObject* utfString = PyUnicode_AsUTF8String((PyObject *) obj);

    Py_buffer pybuf;
    PyObject_GetBuffer(utfString, &pybuf, PyBUF_SIMPLE);

    if(pybuf.buf==NULL)
        return NULL;

    Py_INCREF(utfString);

    return (char*) pybuf.buf;
}


bool Utils::parsePathStringArgAsUTF8(PyObject * args, PyObject *kwds, char* arg_name, char **arg_value){

    PyObject *utfString = NULL, *upath=NULL;

    char** kwlist = new char*[1];
    kwlist[0] = arg_name;

    if (! PyArg_ParseTupleAndKeywords(args, kwds, "O", kwlist, &upath)) {
        std::cerr << "\n ERRORE \n";
        return NULL;
    }

    utfString = upath;
    if(PyUnicode_Check(upath))
        utfString = PyUnicode_AsUTF8String((PyObject *) upath);

    Py_buffer pybuf;
    PyObject_GetBuffer(utfString, &pybuf, PyBUF_SIMPLE);

    if(pybuf.buf==NULL)
        return NULL;

    Py_XDECREF(utfString);
    Py_DECREF(utfString);

    *arg_value = (char*) pybuf.buf;

    return true;
}
