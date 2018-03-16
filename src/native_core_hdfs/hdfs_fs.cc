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

#include "hdfs_fs.h"
#include "hdfs_file.h"

#include <sstream>
#include <hdfs.h>
#include <unicodeobject.h>
#include <errno.h>
#include <string.h>

#define MAX_WD_BUFFSIZE 2048

#define str_empty(s) ((s) == NULL || (*(s) == '\0'))

PyObject* FsClass_new(PyTypeObject *type, PyObject *args, PyObject *kwds)
{
    FsInfo *self;

    self = (FsInfo *)type->tp_alloc(type, 0);
    if (self != NULL) {

        self->host = NULL;
        self->port = 0;

        self->user = NULL;
        self->group = NULL;

        self->_fs = NULL;
    }

    return (PyObject *)self;
}


void FsClass_dealloc(FsInfo* self)
{
    Py_TYPE(self)->tp_free((PyObject*)self);
}


int FsClass_init(FsInfo *self, PyObject *args, PyObject *kwds)
{

    // XXX: This call to PyArg_ParseTuple doesn't support non-ASCII characters in
    // the input strings (host, user, group)
    if (! PyArg_ParseTuple(args, "z|izz",
            &(self->host), &(self->port),
            &(self->user), &(self->group)))
        return -1;

    if (str_empty(self->host))
        self->host = NULL;

    if (str_empty(self->user))
        self->user = NULL;

    if (str_empty(self->group))
        self->group = NULL;

    // Connect cycles and retries more than once if necessary.  Better let
    // other Python threads through.
    Py_BEGIN_ALLOW_THREADS;
        if (self->user != NULL) {
            self->_fs = hdfsConnectAsUser(self->host, self->port, self->user);

        } else {
            self->_fs = hdfsConnect(self->host, self->port);
        }
    Py_END_ALLOW_THREADS;

    if (!self->_fs) {
        PyErr_SetFromErrno(PyExc_RuntimeError);
        return -1;
    }

    return 0;
}


PyObject* FsClass_close(FsInfo* self)
{
    hdfsDisconnect(self->_fs);
    Py_RETURN_NONE;
}


PyObject* FsClass_get_working_directory(FsInfo* self) {

    const size_t bufferSize = MAX_WD_BUFFSIZE;
    char *buffer = (char*)PyMem_Malloc(bufferSize);
    if (!buffer)
        return PyErr_NoMemory();

    if (hdfsGetWorkingDirectory(self->_fs, buffer, bufferSize) == NULL) {
        PyErr_SetString(PyExc_RuntimeError, "Cannot get working directory.");
        PyMem_Free(buffer);
        return NULL;
    }

    PyObject* result = PyUnicode_FromString(buffer);
    PyMem_Free(buffer);
    if (!result)
        return PyErr_NoMemory();

    return result;
}

PyObject* FsClass_get_path_info(FsInfo* self, PyObject *args, PyObject *kwds) {

    char* path = NULL;
    PyObject* retval = NULL;
    hdfsFileInfo* info;

    if (!PyArg_ParseTuple(args, "es", "utf-8",  &path)) {
        return NULL;
    }

    if (str_empty(path)) {
        PyMem_Free(path);
        PyErr_SetString(PyExc_ValueError, "Empty path");
        return NULL;
    }

    Py_BEGIN_ALLOW_THREADS;
        info = hdfsGetPathInfo(self->_fs, path);
    Py_END_ALLOW_THREADS;
    if (info == NULL) {
        PyMem_Free(path);
        return PyErr_SetFromErrno(PyExc_IOError);
    }

    retval =
        Py_BuildValue("{s:O,s:s,s:s,s:i,s:i,s:h,s:s,s:h,s:i,s:O,s:L}",
            "name", PyUnicode_FromString(info->mName),
            "kind", info->mKind == kObjectKindDirectory ? "directory" : "file",
            "group", info->mGroup,
            "last_mod", info->mLastMod,
            "last_access", info->mLastAccess,
            "replication", info->mReplication,
            "owner", info->mOwner,
            "permissions", info->mPermissions,
            "block_size", info->mBlockSize,
            "path", PyUnicode_FromString(info->mName),
            "size", info->mSize
    );
    PyMem_Free(path);
    hdfsFreeFileInfo(info, 1);
    return retval;
}


PyObject* FsClass_get_hosts(FsInfo* self, PyObject *args, PyObject *kwds) {

    Py_ssize_t start, length;
    PyObject* result = NULL;
    char* path = NULL;
    char*** hosts = NULL;

    if (!PyArg_ParseTuple(args, "esnn", "utf-8", &path, &start, &length)) {
        return NULL;
    }

    if (str_empty(path)) {
        PyMem_Free(path);
        PyErr_SetString(PyExc_ValueError, "Empty path");
        return NULL;
    }

    if (start < 0 || length < 0) {
        PyMem_Free(path);
        PyErr_SetString(PyExc_ValueError,
                        "Start position and length must be >= 0");
        return NULL;
    }

    Py_BEGIN_ALLOW_THREADS;
        hosts = hdfsGetHosts(self->_fs, path, start, length);
    Py_END_ALLOW_THREADS;
    PyMem_Free(path);
    if (!hosts) {
        return PyErr_SetFromErrno(PyExc_RuntimeError);
    }

    result = PyList_New(0);
    if (!result) goto mem_error;

    for (int blockNumber = 0; hosts[blockNumber] != NULL; ++blockNumber)
    {
        PyObject* blockHosts = PyList_New(0);
        if (!blockHosts) goto mem_error;

        for (int iBlockHost = 0; hosts[blockNumber][iBlockHost] != NULL; ++iBlockHost)
        {
            PyObject* str = PyUnicode_FromString(hosts[blockNumber][iBlockHost]);
            if (!str) goto mem_error;
            if (PyList_Append(blockHosts, str) < 0) goto mem_error;
        }

        if (PyList_Append(result, blockHosts) < 0) goto mem_error;
    }
    goto done; // skip the mem_error section

mem_error:
    PyErr_SetString(PyExc_MemoryError, "Error allocating host structure");
    Py_XDECREF(result);
    result = NULL;
    // fall through
done:
    if (hosts) hdfsFreeHosts(hosts);
    return result;
}

PyObject* FsClass_get_default_block_size(FsInfo* self) {
    tOffset size = hdfsGetDefaultBlockSize(self->_fs);
    return PyLong_FromSsize_t(size);
}

PyObject* FsClass_get_used(FsInfo* self) {

    tOffset size = hdfsGetUsed(self->_fs);
    return PyLong_FromSsize_t(size);
}

PyObject* FsClass_set_replication(FsInfo* self, PyObject* args, PyObject* kwds) {

    char* path = NULL;
    short replication;
    int result;

    if (!PyArg_ParseTuple(args, "esh", "utf-8", &path, &replication))
        return NULL;

    if (str_empty(path)) {
        PyMem_Free(path);
        PyErr_SetString(PyExc_ValueError, "Empty path");
        return NULL;
    }

    Py_BEGIN_ALLOW_THREADS;
        result = hdfsSetReplication(self->_fs, path, replication);
    Py_END_ALLOW_THREADS;
    PyMem_Free(path);
    if (result < 0) {
	return PyErr_SetFromErrno(PyExc_IOError);
    }
    Py_RETURN_NONE;
}


PyObject* FsClass_set_working_directory(FsInfo* self, PyObject* args, PyObject* kwds) {

    char* path = NULL;
    int result;

    if (!PyArg_ParseTuple(args, "es", "utf-8", &path))
        return NULL;

    if (str_empty(path)) {
        PyMem_Free(path);
        PyErr_SetString(PyExc_ValueError, "Empty path");
        return NULL;
    }

    Py_BEGIN_ALLOW_THREADS;
        result = hdfsSetWorkingDirectory(self->_fs, path);
    Py_END_ALLOW_THREADS;
    PyMem_Free(path);
    if (result < 0) {
	return PyErr_SetFromErrno(PyExc_IOError);
    }
    Py_RETURN_NONE;
}


PyObject* FsClass_open_file(FsInfo* self, PyObject *args, PyObject *kwds)
{
    PyObject* retval = NULL;
    char* path = NULL;
    const char* mode = NULL;
    int flags, buff_size, blocksize;
    short replication;
    hdfsFile file;
    tOffset size = 0;
    hdfsFileInfo* info;

    if (!PyArg_ParseTuple(args, "es|sihi",
                          "utf-8", &path, &mode, &buff_size, &replication,
                          &blocksize)) {
        return NULL;
    }

    if (str_empty(path)) {
        PyMem_Free(path);
        PyErr_SetString(PyExc_ValueError, "Empty path");
        return NULL;
    }

    if (strcmp(mode, MODE_READ) == 0) {
        flags = O_RDONLY;
    } else if (strcmp(mode, MODE_WRITE) == 0) {
        flags = O_WRONLY;
    } else if (strcmp(mode, MODE_APPEND) == 0) {
        flags = O_WRONLY | O_APPEND;
    } else {
        PyMem_Free(path);
        PyErr_SetString(PyExc_ValueError, "Invalid mode");
        return NULL;
    }

    Py_BEGIN_ALLOW_THREADS;
        file = hdfsOpenFile(self->_fs, path, flags,
                            buff_size, replication, blocksize);
    Py_END_ALLOW_THREADS;
    if (file == NULL) {
        PyMem_Free(path);
        return PyErr_SetFromErrno(PyExc_IOError);
    }

    PyObject* module = PyImport_ImportModule("pydoop.native_core_hdfs");
    if (NULL == module) {
        PyMem_Free(path);
	free(file);
	return NULL;
    }
    PyObject *name = PyUnicode_FromString(path);
    PyObject *pymode = PyUnicode_FromString(mode);
    retval = PyObject_CallMethod(module, "CoreHdfsFile", "OOOO",
				 self->_fs, file, name, pymode);
    Py_XDECREF(pymode);
    Py_XDECREF(name);
    Py_XDECREF(module);
    if (NULL == retval) {
        PyMem_Free(path);
        free(file);
        return NULL;
    }

    /* get file size for the SEEK_END variant of seek */
    if (flags == O_RDONLY) {
        Py_BEGIN_ALLOW_THREADS;
            info = hdfsGetPathInfo(self->_fs, path);
        Py_END_ALLOW_THREADS;
        if (info == NULL) {
            PyMem_Free(path);
            return PyErr_SetFromErrno(PyExc_IOError);
        }
        size = info->mSize;
        hdfsFreeFileInfo(info, 1);
    }
    PyMem_Free(path);

    FileInfo *fileInfo = ((FileInfo*) retval);
    fileInfo->size = size;
    fileInfo->buff_size = buff_size;
    fileInfo->blocksize = blocksize;
    fileInfo->replication = replication;
    return retval;
}


PyObject *FsClass_get_capacity(FsInfo *self) {
    tOffset capacity;

    Py_BEGIN_ALLOW_THREADS;
        errno = 0; // hdfsGetCapacity forgets to clear errno
        capacity = hdfsGetCapacity(self->_fs);
    Py_END_ALLOW_THREADS;

    if (capacity < 0) {
        // two error cases are contemplated by the code in hdfsGetCapacity:
        // 1) exception from the Java method
        // 2) FS instance is not a DistributedFileSystem.
        // Here we copy their error textually.
        if (errno)
            PyErr_SetFromErrno(PyExc_IOError);
        else {
            PyErr_SetString(PyExc_RuntimeError,
                    "hdfsGetCapacity works only on a DistributedFileSystem");
        }

        return NULL;
    }
    return PyLong_FromSsize_t(capacity);
}


PyObject* FsClass_copy(FsInfo* self, PyObject *args, PyObject *kwds)
{
    FsInfo* to_hdfs;
    char *from_path = NULL, *to_path = NULL;
    int result;

    if (! PyArg_ParseTuple(args, "esOes", "utf-8", &from_path,
                &to_hdfs, "utf-8", &to_path)) {
        return NULL;
    }

    if (str_empty(from_path) || str_empty(to_path)) {
        PyMem_Free(from_path);
        PyMem_Free(to_path);
        PyErr_SetString(PyExc_ValueError, "Empty path");
        return NULL;
    }

    Py_BEGIN_ALLOW_THREADS;
        result = hdfsCopy(self->_fs, from_path, to_hdfs->_fs, to_path);
    Py_END_ALLOW_THREADS;
    PyMem_Free(from_path);
    PyMem_Free(to_path);
    if (result < 0) {
        return PyErr_SetFromErrno(PyExc_IOError);
    }
    return PyLong_FromLong(result);
}


PyObject *FsClass_exists(FsInfo *self, PyObject *args, PyObject *kwds) {

    char* path = NULL;
    int result;

    if (! PyArg_ParseTuple(args, "es", "utf-8", &path))
        return NULL;

    if (str_empty(path)) {
        PyMem_Free(path);
        PyErr_SetString(PyExc_ValueError, "Empty path");
        return NULL;
    }

    Py_BEGIN_ALLOW_THREADS;
        result = hdfsExists(self->_fs, path);
    Py_END_ALLOW_THREADS;
    PyMem_Free(path);

    // LP: hdfsExists (in some cases?) sets errno to ENOENT "[Errno 2] No such
    // file or directory" when the path doesn't exist or EEXIST in other cases.
    // I don't know why.  Since that's what we're trying to test, I'll skip
    // checking errno here.  The consequence is that when we return false it
    // may be because of an error and not because the path doesn't exist.
    //
    // if (result < 0 && errno) return PyErr_SetFromErrno(PyExc_IOError);

    return PyBool_FromLong(result >= 0 ? 1 : 0);
}


PyObject *FsClass_create_directory(FsInfo *self, PyObject *args, PyObject *kwds) {

    char* path = NULL;
    int result;

    if (! PyArg_ParseTuple(args, "es", "utf-8", &path)) {
        return NULL;
    }

    if (str_empty(path)) {
        PyMem_Free(path);
        PyErr_SetString(PyExc_ValueError, "Empty path");
        return NULL;
    }

    Py_BEGIN_ALLOW_THREADS;
        result = hdfsCreateDirectory(self->_fs, path);
    Py_END_ALLOW_THREADS;
    PyMem_Free(path);
    if (result < 0) {
        return PyErr_SetFromErrno(PyExc_IOError);
    }
    Py_RETURN_NONE;
}

/*
 * Works on borrowed reference `dict`.
 *
 * \return 0 if successful
 * \return -1 if there was a problem. In that case, dict may contain
 * some values, but will be incomplete and should be discarded.
 */
static int setPathInfo(PyObject* dict, hdfsFileInfo* fileInfo) {

    if (dict == NULL || fileInfo == NULL) return -1;
    int error_code = 0;

    const char*const keys[] = {
        "name",
        "kind",
        "group",
        "last_mod",
        "last_access",
        "replication",
        "owner",
        "permissions",
        "block_size",
        "path",
        "size"
    };

    const int n_fields = sizeof(keys) / sizeof(keys[0]);

    PyObject* values[n_fields];
    int i = 0;
    // Prepare the values.  We'll check for all errors in the "set" loop below
    // The order of these values MUST match the order of the keys above
    values[i++] = PyUnicode_FromString(fileInfo->mName);
    values[i++] = PyUnicode_FromString(fileInfo->mKind == kObjectKindDirectory ? "directory" : "file");
    values[i++] = PyUnicode_FromString(fileInfo->mGroup);
    values[i++] = PyLong_FromLong(fileInfo->mLastMod);
    values[i++] = PyLong_FromLong(fileInfo->mLastAccess);
    values[i++] = PyLong_FromSize_t(fileInfo->mReplication);
    values[i++] = PyUnicode_FromString(fileInfo->mOwner);
    values[i++] = PyLong_FromSize_t(fileInfo->mPermissions);
    values[i++] = PyLong_FromLong(fileInfo->mBlockSize);
    values[i++] = PyUnicode_FromString(fileInfo->mName);
    values[i++] = PyLong_FromLongLong(fileInfo->mSize);

    for (i = 0; i < n_fields; ++i) {
        if (values[i] == NULL || PyDict_SetItemString(dict, keys[i], values[i]) < 0) {
            error_code = -1;
            break;
            // Don't DECREF here.  The error handling code goes through the entire array
            // and thus we'd end up DECREFing some objects twice.
        }
    }

    for (i = 0; i < n_fields; ++i) {
        Py_XDECREF(values[i]); // some values may be null (if there was an error
    }

    return error_code;
}

PyObject *FsClass_list_directory(FsInfo *self, PyObject *args, PyObject *kwds) {

    PyObject* retval = NULL;
    char* path = NULL;

    hdfsFileInfo* pathList = NULL;
    int numEntries = 0;
    hdfsFileInfo* pathInfo = NULL;

    if (!PyArg_ParseTuple(args, "es", "utf-8",  &path))
        return NULL;

    if (str_empty(path)) {
        PyMem_Free(path);
        PyErr_SetString(PyExc_ValueError, "Empty path");
        return NULL;
    }

    Py_BEGIN_ALLOW_THREADS;
        pathInfo = hdfsGetPathInfo(self->_fs, path);
        PyMem_Free(path);
        if (!pathInfo) {
            Py_BLOCK_THREADS; // later we 'goto' skipping over END_ALLOW_THREADS
            PyErr_SetFromErrno(PyExc_IOError);
            goto error;
        }

        if (pathInfo->mKind == kObjectKindDirectory) {

            pathList = hdfsListDirectory(self->_fs, pathInfo->mName, &numEntries);

            // hdfsListDirectory returns NULL when a directory is empty, so to determine
            // whether there's been an error we also need to check errno
            if (!pathList && errno) {
                Py_BLOCK_THREADS; // later we 'goto' skipping over END_ALLOW_THREADS
                PyErr_SetFromErrno(PyExc_IOError);
                goto error;
            }
        }
        else {
            numEntries = 1;
            pathList = pathInfo;
            pathInfo = NULL;
        }
    Py_END_ALLOW_THREADS;

    retval = PyList_New(numEntries);
    if (!retval) goto mem_error;

    for (Py_ssize_t i = 0; i < numEntries; i++) {
        PyObject* infoDict = PyDict_New();
        if (!infoDict) goto mem_error;
        PyList_SET_ITEM(retval, i, infoDict);
        if (setPathInfo(infoDict, &pathList[i]) < 0) {
            PyErr_SetString(PyExc_IOError, "Error getting file info");
            goto error;
        }
    }

    goto done; // skip the error section

mem_error:
    PyErr_SetString(PyExc_MemoryError, "Error allocating structures");
    // fall through
error:
    // in case of error DECREF our retval structure and return NULL
    if (retval != NULL) {
        Py_XDECREF(retval);
        retval = NULL;
    }

done:
    // all code paths go through the 'done' section
    if (pathInfo != NULL)
        hdfsFreeFileInfo(pathInfo, 1);
    if (pathList != NULL)
        hdfsFreeFileInfo(pathList, numEntries);

    return retval;
}

PyObject *FsClass_move(FsInfo *self, PyObject *args, PyObject *kwds) {

    FsInfo* to_hdfs;
    char *from_path = NULL, *to_path = NULL;
    int result;

    if (! PyArg_ParseTuple(args, "esOes", "utf-8", &from_path,
                &to_hdfs, "utf-8", &to_path)) {
        return NULL;
    }

    if (str_empty(from_path) || str_empty(to_path)) {
        PyMem_Free(from_path);
        PyMem_Free(to_path);
        PyErr_SetString(PyExc_ValueError, "Empty path");
        return NULL;
    }

    Py_BEGIN_ALLOW_THREADS;
        result = hdfsMove(self->_fs, from_path, to_hdfs->_fs, to_path);
    Py_END_ALLOW_THREADS;
    PyMem_Free(from_path);
    PyMem_Free(to_path);

    if (result < 0) {
        return PyErr_SetFromErrno(PyExc_IOError);
    }
    Py_RETURN_NONE;
}


PyObject *FsClass_rename(FsInfo *self, PyObject *args, PyObject *kwds) {

    char *from_path = NULL, *to_path = NULL;
    int result;

    if (! PyArg_ParseTuple(args, "eses", "utf-8", &from_path, "utf-8", &to_path))
        return NULL;

    if (str_empty(from_path) || str_empty(to_path)) {
        PyMem_Free(from_path);
        PyMem_Free(to_path);
        PyErr_SetString(PyExc_ValueError, "Empty path");
        return NULL;
    }

    Py_BEGIN_ALLOW_THREADS;
        result = hdfsRename(self->_fs, from_path, to_path);
    Py_END_ALLOW_THREADS;
    PyMem_Free(from_path);
    PyMem_Free(to_path);
    if (result < 0) {
        return PyErr_SetFromErrno(PyExc_IOError);
    }
    Py_RETURN_NONE;
}


PyObject *FsClass_delete(FsInfo *self, PyObject *args, PyObject *kwds) {

    char* path = NULL;
    int recursive = 1;
    int result;

    if (!PyArg_ParseTuple(args, "es|i", "utf-8", &path, &recursive)) {
        return NULL;
    }

    if (str_empty(path)) {
        PyMem_Free(path);
        PyErr_SetString(PyExc_ValueError, "Empty path");
        return NULL;
    }

    Py_BEGIN_ALLOW_THREADS;
        result = hdfsDelete(self->_fs, path, recursive);
    Py_END_ALLOW_THREADS;
    PyMem_Free(path);

    if (result < 0) {
        return PyErr_SetFromErrno(PyExc_IOError);
    }
    Py_RETURN_NONE;
}


PyObject *FsClass_chmod(FsInfo *self, PyObject *args, PyObject *kwds) {

    char* path = NULL;
    short mode = 1;
    int result;

    if (!PyArg_ParseTuple(args, "esh", "utf-8", &path, &mode)) {
        return NULL;
    }

    if (str_empty(path)) {
        PyMem_Free(path);
        PyErr_SetString(PyExc_ValueError, "Empty path");
        return NULL;
    }

    Py_BEGIN_ALLOW_THREADS;
        // hdfsChmod doesn't always set errno in case of error.  We clear it
        // here so that after the call we'll be sure we're not looking at an old value
        errno = 0;
        result = hdfsChmod(self->_fs, path, mode);
    Py_END_ALLOW_THREADS;
    PyMem_Free(path);

    if (result >= 0) {
        Py_RETURN_NONE;
    }
    else {
        // there's been an error
        if (errno) {
            return PyErr_SetFromErrno(PyExc_IOError);
	}
        else {
            PyErr_SetString(PyExc_IOError, "Unknown error");
	    return NULL;
        }
    }
}


PyObject *FsClass_chown(FsInfo *self, PyObject *args, PyObject *kwds) {

    char *path = NULL, *input_user = NULL, *input_group = NULL;
    int result;
    hdfsFileInfo* fileInfo;

    if (! PyArg_ParseTuple(args, "es|eses",
                "utf-8", &path, "utf-8", &input_user, "utf-8", &input_group)) {
        return NULL;
    }

    if (str_empty(path)) {
        PyMem_Free(path);
        PyMem_Free(input_user);
        PyMem_Free(input_group);
        PyErr_SetString(PyExc_ValueError, "Empty path");
        return NULL;
    }

    Py_BEGIN_ALLOW_THREADS;
        fileInfo = hdfsGetPathInfo(self->_fs, path);
        if (NULL == fileInfo) {
            PyMem_Free(path);
            PyMem_Free(input_user);
            PyMem_Free(input_group);
	    return PyErr_SetFromErrno(PyExc_IOError);
        }
        const char* new_user = str_empty(input_user) ? fileInfo->mOwner : input_user;
        const char* new_group = str_empty(input_group) ? fileInfo->mGroup : input_group;
        result = hdfsChown(self->_fs, path, new_user, new_group);
    Py_END_ALLOW_THREADS;
    PyMem_Free(path);
    PyMem_Free(input_user);
    PyMem_Free(input_group);
    hdfsFreeFileInfo(fileInfo, 1);
    if (result < 0) {
        return PyErr_SetFromErrno(PyExc_IOError);
    }
    Py_RETURN_NONE;
}


PyObject *FsClass_utime(FsInfo *self, PyObject *args, PyObject *kwds) {

    char* path = NULL;
    tTime mtime, atime;
    int result;

    if (! PyArg_ParseTuple(args, "esll", "utf-8", &path, &mtime, &atime)) {
        return NULL;
    }

    if (str_empty(path)) {
        PyMem_Free(path);
        PyErr_SetString(PyExc_ValueError, "Empty path");
        return NULL;
    }

    Py_BEGIN_ALLOW_THREADS;
        result = hdfsUtime(self->_fs, path, mtime, atime);
    Py_END_ALLOW_THREADS;
    PyMem_Free(path);

    if (result < 0) {
        return PyErr_SetFromErrno(PyExc_IOError);
    }
    Py_RETURN_NONE;
}
