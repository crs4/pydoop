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

#include "hdfs_file.h"
#include <stdio.h>

#define PYDOOP_TEXT_ENCODING  "utf-8"


PyObject* FileClass_new(PyTypeObject *type, PyObject *args, PyObject *kwds)
{
    FileInfo *self;

    self = (FileInfo *)type->tp_alloc(type, 0);
    if (self != NULL) {
        self->fs = NULL;
        self->file = NULL;
        if (NULL == (self->name = PyUnicode_FromString(""))) {
            Py_DECREF(self);
            return NULL;
        }
        if (NULL == (self->mode = PyUnicode_FromString(""))) {
            Py_DECREF(self);
            return NULL;
        }
        self->size = 0;
        self->buff_size = 0;
        self->replication = 1;
        self->blocksize = 0;
        self->closed = 0;
    }
    return (PyObject *)self;
}


void FileClass_dealloc(FileInfo* self)
{
    self->file = NULL;
    Py_TYPE(self)->tp_free((PyObject*)self);
}


int FileClass_init(FileInfo *self, PyObject *args, PyObject *kwds)
{
    PyObject *name = NULL, *mode = NULL, *tmp;

    if (!PyArg_ParseTuple(args, "OOOO",
                          &(self->fs), &(self->file), &name, &mode)) {
        return -1;
    }

    if (name) {
	tmp = self->name;
	Py_INCREF(name);
	self->name = name;
	Py_XDECREF(tmp);
    }
    if (mode) {
	tmp = self->mode;
	Py_INCREF(mode);
	self->mode = mode;
	Py_XDECREF(tmp);
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
    } else {
        self->closed = 1;
        return PyBool_FromLong(1);
    }
}


PyObject* FileClass_getclosed(FileInfo* self, void* closure) {
  return PyBool_FromLong(self->closed);
}


PyObject* FileClass_getbuff_size(FileInfo* self, void* closure) {
  return PyLong_FromLong(self->buff_size);
}


PyObject* FileClass_getname(FileInfo* self, void* closure) {
    Py_INCREF(self->name);
    return self->name;
}


PyObject* FileClass_getmode(FileInfo* self, void* closure) {
    Py_INCREF(self->mode);
    return self->mode;
}


PyObject* FileClass_readable(FileInfo* self) {
  return PyBool_FromLong(hdfsFileIsOpenForRead(self->file));
}


PyObject* FileClass_writable(FileInfo* self) {
  return PyBool_FromLong(hdfsFileIsOpenForWrite(self->file));
}


PyObject* FileClass_seekable(FileInfo* self) {
  return PyBool_FromLong(hdfsFileIsOpenForRead(self->file));
}


PyObject* FileClass_available(FileInfo *self){
    int available = hdfsAvailable(self->fs, self->file);
    if (available < 0)
        return PyErr_SetFromErrno(PyExc_IOError);
    else
        return PyLong_FromLong(available);
}

static int _ensure_open_for_reading(FileInfo* self) {
    if (!hdfsFileIsOpenForRead(self->file)) {
        PyErr_SetString(PyExc_IOError, "File is not opened in READ ('r') mode");
        return 0; // False
    }

    return 1; // True
}

static Py_ssize_t _read_into_pybuf(FileInfo *self, char* buf, Py_ssize_t nbytes) {

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

static PyObject* _read_new_pybuf(FileInfo* self, Py_ssize_t nbytes) {

    if (nbytes < 0) {
        PyErr_SetString(PyExc_ValueError, "nbytes must be >= 0");
        return NULL;
    }

    // Allocate an uninitialized buffer object.
    // We then access and directly modify the buffer's internal memory. This is
    // ok until we release this string "into the wild".

    PyObject* retval = _PyBuf_FromStringAndSize(NULL, nbytes);    
    if (!retval) return PyErr_NoMemory();

    Py_ssize_t bytes_read = _read_into_pybuf(self, _PyBuf_AS_STRING(retval),
                                             nbytes);

    if (bytes_read >= 0) {
        // If bytes_read >= 0, read worked properly. But, if bytes_read < nbytes
        // we got fewer bytes than requested (maybe we reached EOF?).  We need
        // to shrink the string to the correct length.  In case of error the
        // call to _PyString_Resize frees the original string, sets the
        // appropriate python exception and returns -1.
        if (bytes_read >= nbytes || _PyBuf_Resize(&retval, bytes_read) >= 0)  
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
static Py_ssize_t _pread_into_pybuf(FileInfo *self, char* buffer, Py_ssize_t pos,
                                    Py_ssize_t nbytes) {

    Py_ssize_t orig_position = hdfsTell(self->fs, self->file);
    if (orig_position < 0) {
        PyErr_SetFromErrno(PyExc_IOError);
        return -1;
    }

    if (hdfsSeek(self->fs, self->file, pos) < 0) {
        PyErr_SetFromErrno(PyExc_IOError);
        return -1;
    }

    tSize bytes_read = _read_into_pybuf(self, buffer, nbytes);

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

static PyObject* _pread_new_pybuf(FileInfo* self, Py_ssize_t pos, Py_ssize_t nbytes) {

    if (nbytes < 0) {
        PyErr_SetString(PyExc_ValueError, "nbytes must be >= 0");
        return NULL;
    }

    // Allocate an uninitialized string object.
    PyObject* retval = _PyBuf_FromStringAndSize(NULL, nbytes);    
    if (!retval) return PyErr_NoMemory();

    Py_ssize_t bytes_read = _pread_into_pybuf(self, _PyBuf_AS_STRING(retval),
                                              pos, nbytes);

    if (bytes_read >= 0) {
        // If bytes_read >= 0, read worked properly. But, if bytes_read < nbytes
        // we got fewer bytes than requested (maybe we reached EOF?).  We need
        // to shrink the string to the correct length.  In case of error the
        // call to _PyString_Resize frees the original string, sets the
        // appropriate python exception and returns -1.
        if (bytes_read >= nbytes || _PyBuf_Resize(&retval, bytes_read) >= 0)
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
      return _PyBuf_FromString("");
    }
    // else nbytes > 0

    return _read_new_pybuf(self, nbytes);
}


PyObject* FileClass_read_chunk(FileInfo *self, PyObject *args, PyObject *kwds){

    Py_buffer buffer;

    if (!_ensure_open_for_reading(self))
        return NULL;

    if (! PyArg_ParseTuple(args, "w*",  &buffer))
        return NULL;

    Py_ssize_t bytes_read = _read_into_pybuf(self, (char*)buffer.buf, buffer.len);
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
        errno = EINVAL;
        PyErr_SetFromErrno(PyExc_IOError);
        errno = 0;
        return NULL;
    }

    if (nbytes == 0)
      return _PyBuf_FromString("");

    // else

    return _pread_new_pybuf(self, position, nbytes);
}


PyObject* FileClass_pread_chunk(FileInfo *self, PyObject *args, PyObject *kwds){

    Py_buffer buffer;
    Py_ssize_t position;

    if (!_ensure_open_for_reading(self))
        return NULL;

    if (! PyArg_ParseTuple(args, "nw*", &position, &buffer))
        return NULL;

    if (position < 0) {
        errno = EINVAL;
        PyErr_SetFromErrno(PyExc_IOError);
        errno = 0;
        return NULL;
    }

    Py_ssize_t bytes_read = _pread_into_pybuf(self, (char*)buffer.buf, position,
                                              buffer.len);
    PyBuffer_Release(&buffer);

    if (bytes_read >= 0)
        return Py_BuildValue("n", bytes_read);
    else
        return NULL;
}


PyObject* FileClass_seek(FileInfo *self, PyObject *args, PyObject *kwds) {

    tOffset position, curpos;
    int whence = SEEK_SET;

    if (!PyArg_ParseTuple(args, "n|i", &position, &whence))
        return NULL;

    switch (whence) {
    case SEEK_SET:
        break;
    case SEEK_CUR:
        curpos = hdfsTell(self->fs, self->file);
        if (curpos < 0) {
            return PyErr_SetFromErrno(PyExc_IOError);
        }
        position += curpos;
        break;
    case SEEK_END:
        position += self->size;
        break;
    default:
        PyErr_SetString(PyExc_ValueError, "unsupported whence value");
        return NULL;
    }

    /* HDFS does not support seeking past end of file */
    if (position < 0 || position > self->size) {
        errno = EINVAL;
        PyErr_SetFromErrno(PyExc_IOError);
        errno = 0;
        return NULL;
    }

    if (hdfsSeek(self->fs, self->file, position) < 0) {
	return PyErr_SetFromErrno(PyExc_IOError);
    }
    return PyLong_FromLong(position);
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



PyObject* FileClass_write(FileInfo* self, PyObject *args, PyObject *kwds) {
    PyObject *input;
    Py_buffer buffer;

    if (!hdfsFileIsOpenForWrite(self->file)) {
        PyErr_SetString(PyExc_IOError, "not writable");
        return NULL;
    }
    if (!PyArg_ParseTuple(args, "O",  &input)) {
        return NULL;
    }
    if (PyObject_GetBuffer(input, &buffer, PyBUF_SIMPLE) < 0) {
        PyErr_SetString(PyExc_TypeError, "Argument not accessible as a buffer");
        return NULL;
    }

    Py_ssize_t written;
    Py_BEGIN_ALLOW_THREADS;
    written = hdfsWrite(self->fs, self->file, buffer.buf, buffer.len);
    Py_END_ALLOW_THREADS;
    PyBuffer_Release(&buffer);
    if (written < 0) {
        PyErr_SetFromErrno(PyExc_IOError);
        return NULL;
    }
    return Py_BuildValue("n", written);
}


PyObject* FileClass_flush(FileInfo *self){
    if (!hdfsFileIsOpenForWrite(self->file)) {
      Py_RETURN_NONE;
    }
    int result = hdfsFlush(self->fs, self->file);

    if (result >= 0) {
        Py_RETURN_NONE;
    }
    else {
        PyErr_SetFromErrno(PyExc_IOError);
        return NULL;
    }
}
