#include "hdfs_utils.h"
#include <iostream>
#include <stdarg.h>
#include <unicodeobject.h>

using namespace hdfs4python;

using namespace std;


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
        cerr << "\n ERRORE \n";
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