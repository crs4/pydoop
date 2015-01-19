/* BEGIN_COPYRIGHT
 *
 * Copyright 2009-2015 CRS4.
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

#include "hdfs_file.h"

#define PYDOOP_TEXT_ENCODING  "utf-8"

PyObject* FileClass_new(PyTypeObject *type, PyObject *args, PyObject *kwds)
{
    FileInfo *self;

    self = (FileInfo *)type->tp_alloc(type, 0);
    if (self != NULL) {
        self->fs = NULL;
        self->file = NULL;
        self->flags = 0;
        self->buff_size = 0;
        self->replication = 1;
        self->blocksize = 0;
        self->readline_chunk_size = 16 * 1024; // 16 KB
#ifdef HADOOP_LIBHDFS_V1
        self->stream_type = UNINITIALIZED;
#endif
    }
    return (PyObject *)self;
}


#ifdef HADOOP_LIBHDFS_V1

static bool hdfsFileIsOpenForWrite(FileInfo *f){
    return f->stream_type == OUTPUT;
}


static bool hdfsFileIsOpenForRead(FileInfo *f){
    return f->stream_type == INPUT;
}

#endif

void FileClass_dealloc(FileInfo* self)
{
    self->file = NULL;
    self->ob_type->tp_free((PyObject*)self);
}


int FileClass_init(FileInfo *self, PyObject *args, PyObject *kwds)
{
    if (! PyArg_ParseTuple(args, "OO", &(self->fs), &(self->file))) {
        return -1;
    }

    return 0;
}


int FileClass_init_internal(FileInfo *self, hdfsFS fs, hdfsFile file)
{
    self->fs = fs;
    self->file = file;

    return 0;
}


PyObject* FileClass_close(FileInfo* self){
    int result = hdfsCloseFile(self->fs, self->file);
    if (result < 0) {
        return PyErr_SetFromErrno(PyExc_IOError);
    }
    else
        return PyBool_FromLong(1);
}


PyObject* FileClass_available(FileInfo *self){
    int available = hdfsAvailable(self->fs, self->file);
    if (available < 0)
        return PyErr_SetFromErrno(PyExc_IOError);
    else
        return PyLong_FromLong(available);
}

static int _ensure_open_for_reading(FileInfo* self) {
    #ifdef HADOOP_LIBHDFS_V1
    if(!hdfsFileIsOpenForRead(self)){
    #else
    if(!hdfsFileIsOpenForRead(self->file)){
    #endif
        PyErr_SetString(PyExc_IOError, "File is not opened in READ ('r') mode");
        return 0; // False
    }

    return 1; // True
}

static int _ensure_open_for_writing(FileInfo* self) {
    #ifdef HADOOP_LIBHDFS_V1
    if(!hdfsFileIsOpenForWrite(self)){
    #else
    if(!hdfsFileIsOpenForWrite(self->file)){
    #endif
        PyErr_SetString(PyExc_IOError, "File is not opened in WRITE ('w') mode");
        return 0; // False
    }

    return 1; // True
}

static Py_ssize_t _read_into_str(FileInfo *self, char* buf, Py_ssize_t nbytes) {

    if (nbytes < 0) {
        PyErr_SetString(PyExc_ValueError, "nbytes must be >= 0");
        return -1;
    }

    tSize bytes_read;
    Py_BEGIN_ALLOW_THREADS;
        bytes_read = hdfsRead(self->fs, self->file, buf, nbytes);
    Py_END_ALLOW_THREADS;

    if (bytes_read < 0) { // error
        PyErr_SetFromErrno(PyExc_IOError);
        return -1;
    }

    return bytes_read;
}

static PyObject* _read_new_pystr(FileInfo* self, Py_ssize_t nbytes) {

    if (nbytes < 0) {
        PyErr_SetString(PyExc_ValueError, "nbytes must be >= 0");
        return NULL;
    }

    // Allocate an uninitialized string object.
    // We then access and directly modify the string's internal memory. This is
    // ok until we release this string "into the wild".
    PyObject* retval = PyString_FromStringAndSize(NULL, nbytes);
    if (!retval) return PyErr_NoMemory();

    Py_ssize_t bytes_read = _read_into_str(self, PyString_AS_STRING(retval), nbytes);

    if (bytes_read >= 0) {
        // If bytes_read >= 0, read worked properly. But, if bytes_read < nbytes
        // we got fewer bytes than requested (maybe we reached EOF?).  We need
        // to shrink the string to the correct length.  In case of error the
        // call to _PyString_Resize frees the original string, sets the
        // appropriate python exception and returns -1.
        if (bytes_read >= nbytes || _PyString_Resize(&retval, bytes_read) >= 0)
            return retval; // all good
    }

    // If we get here something's gone wrong.  The exception should already be set.
    Py_DECREF(retval);
    return NULL;
}

/*
 * Seek to `pos` and read `nbytes` bytes into a the provided buffer.
 *
 * \return: Number of bytes read. In case of error this function sets
 * the appropriate Python exception and returns -1.
 */
static Py_ssize_t _pread_into_str(FileInfo *self, char* buffer, Py_ssize_t pos, Py_ssize_t nbytes) {

    Py_ssize_t orig_position = hdfsTell(self->fs, self->file);
    if (orig_position < 0) {
        PyErr_SetFromErrno(PyExc_IOError);
        return -1;
    }

    if (hdfsSeek(self->fs, self->file, pos) < 0) {
        PyErr_SetFromErrno(PyExc_IOError);
        return -1;
    }

    tSize bytes_read = _read_into_str(self, buffer, nbytes);

    if (bytes_read < 0) {
        PyErr_SetFromErrno(PyExc_IOError);
        return -1;
    }

    if (hdfsSeek(self->fs, self->file, orig_position) < 0) {
        PyErr_SetFromErrno(PyExc_IOError);
        return -1;
    }

    return bytes_read;
}

static PyObject* _pread_new_pystr(FileInfo* self, Py_ssize_t pos, Py_ssize_t nbytes) {

    if (nbytes < 0) {
        PyErr_SetString(PyExc_ValueError, "nbytes must be >= 0");
        return NULL;
    }

    // Allocate an uninitialized string object.
    PyObject* retval = PyString_FromStringAndSize(NULL, nbytes);
    if (!retval) return PyErr_NoMemory();

    Py_ssize_t bytes_read = _pread_into_str(self, PyString_AS_STRING(retval), pos, nbytes);

    if (bytes_read >= 0) {
        // If bytes_read >= 0, read worked properly. But, if bytes_read < nbytes
        // we got fewer bytes than requested (maybe we reached EOF?).  We need
        // to shrink the string to the correct length.  In case of error the
        // call to _PyString_Resize frees the original string, sets the
        // appropriate python exception and returns -1.
        if (bytes_read >= nbytes || _PyString_Resize(&retval, bytes_read) >= 0)
            return retval; // all good
    }

    // If we get here something's gone wrong.  The exception should already be set.
    Py_DECREF(retval);
    return NULL;
}


PyObject* FileClass_read(FileInfo *self, PyObject *args, PyObject *kwds){

    Py_ssize_t nbytes;

    if (!_ensure_open_for_reading(self))
        return NULL;

    if (! PyArg_ParseTuple(args, "n", &(nbytes)))
        return NULL;

    if (nbytes < 0) {
        PyErr_SetString(PyExc_ValueError, "nbytes must be >= 0");
        return NULL;
    }
    else if (nbytes == 0) {
      return PyString_FromString("");
    }
    // else nbytes > 0

    return _read_new_pystr(self, nbytes);
}


PyObject* FileClass_read_chunk(FileInfo *self, PyObject *args, PyObject *kwds){

    Py_buffer buffer;

    if (!_ensure_open_for_reading(self))
        return NULL;

    if (! PyArg_ParseTuple(args, "w*",  &buffer))
        return NULL;

    Py_ssize_t bytes_read = _read_into_str(self, (char*)buffer.buf, buffer.len);
    PyBuffer_Release(&buffer);

    if (bytes_read >= 0)
        return Py_BuildValue("n", bytes_read);
    else
        return NULL;
}


PyObject* FileClass_pread(FileInfo *self, PyObject *args, PyObject *kwds){

    Py_ssize_t position;
    Py_ssize_t nbytes;

    if (!_ensure_open_for_reading(self))
        return NULL;

    if (! PyArg_ParseTuple(args, "nn", &position, &nbytes))
        return NULL;

    if (position < 0) {
        PyErr_SetString(PyExc_ValueError, "position must be >= 0");
        return NULL;
    }

    if (nbytes == 0)
      return PyString_FromString("");

    // else

    return _pread_new_pystr(self, position, nbytes);
}


PyObject* FileClass_pread_chunk(FileInfo *self, PyObject *args, PyObject *kwds){

    Py_buffer buffer;
    Py_ssize_t position;

    if (!_ensure_open_for_reading(self))
        return NULL;

    if (! PyArg_ParseTuple(args, "nw*", &position, &buffer))
        return NULL;

    if (position < 0) {
        PyErr_SetString(PyExc_ValueError, "position must be >= 0");
        return NULL;
    }

    Py_ssize_t bytes_read = _pread_into_str(self, (char*)buffer.buf, position, buffer.len);
    PyBuffer_Release(&buffer);

    if (bytes_read >= 0)
        return Py_BuildValue("n", bytes_read);
    else
        return NULL;
}


PyObject* FileClass_seek(FileInfo *self, PyObject *args, PyObject *kwds){

    tOffset position;

    if (! PyArg_ParseTuple(args, "n", &position))
        return NULL;

    if (position < 0) {
        // raise an IOError like a regular python file
        errno = EINVAL;
        PyErr_SetFromErrno(PyExc_IOError);
        errno = 0;
        return NULL;
    }

    int result = hdfsSeek(self->fs, self->file, position);
    if (result >= 0)
        Py_RETURN_NONE;
    else {
        PyErr_SetFromErrno(PyExc_IOError);
        return NULL;
    }
}


PyObject* FileClass_tell(FileInfo *self, PyObject *args, PyObject *kwds){

    tOffset offset = hdfsTell(self->fs, self->file);
    if (offset >= 0)
        return Py_BuildValue("n", offset);
    else {
        PyErr_SetFromErrno(PyExc_IOError);
        return NULL;
    }
}



PyObject* FileClass_write(FileInfo* self, PyObject *args, PyObject *kwds)
{
    PyObject *input, *encoded = NULL, *retval = NULL;
    Py_buffer buffer;

    if (!_ensure_open_for_writing(self))
        return NULL;

    if (! PyArg_ParseTuple(args, "O",  &input))
        return NULL;

    /* If we get a unicode object we serialized it using the
     * system's default codec.  Any other object we try to access
     * it directly through a Python buffer.
     */
    if (PyUnicode_Check(input)) {
       encoded = PyUnicode_AsEncodedString(input, PYDOOP_TEXT_ENCODING, NULL);
       if (!encoded) // error
           return NULL;
       input = encoded; // override original input with encoded string
    }

    if (PyObject_GetBuffer(input, &buffer, PyBUF_SIMPLE) < 0) {
        PyErr_SetString(PyExc_TypeError, "Argument is not accessible as a Python buffer");
    }
    else {
        Py_ssize_t written;
        Py_BEGIN_ALLOW_THREADS;
            written = hdfsWrite(self->fs, self->file, buffer.buf, buffer.len);
        Py_END_ALLOW_THREADS;
        PyBuffer_Release(&buffer);

        if (written >= 0)
            retval = Py_BuildValue("n", written);
        else {
            PyErr_SetFromErrno(PyExc_IOError);
            retval = NULL;
        }
    }

    Py_XDECREF(encoded);
    return retval;
}

PyObject* FileClass_flush(FileInfo *self){
    int result = hdfsFlush(self->fs, self->file);

    if (result >= 0) {
        Py_RETURN_NONE;
    }
    else {
        PyErr_SetFromErrno(PyExc_IOError);
        return NULL;
    }
}
