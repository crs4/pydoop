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

#include <structmember.h>
#include "../py3k_compat.h"


typedef struct {
    PyObject_HEAD
    hdfsFS fs;
    hdfsFile file;
    PyObject *name;
    PyObject *mode;
    tOffset size;
    int buff_size;
    short replication;
    int blocksize;
    int closed;
} FileInfo;


PyObject* FileClass_new(PyTypeObject *type, PyObject *args, PyObject *kwds);

void FileClass_dealloc(FileInfo* self);

int FileClass_init(FileInfo *self, PyObject *args, PyObject *kwds);

int FileClass_init_internal(FileInfo *self, hdfsFS fs, hdfsFile file);

PyObject* FileClass_close(FileInfo* self);

PyObject* FileClass_getclosed(FileInfo* self, void* closure);

PyObject* FileClass_getbuff_size(FileInfo* self, void* closure);

PyObject* FileClass_getname(FileInfo* self, void* closure);

PyObject* FileClass_getmode(FileInfo* self, void* closure);

PyObject* FileClass_readable(FileInfo* self);

PyObject* FileClass_writable(FileInfo* self);

PyObject* FileClass_seekable(FileInfo* self);

PyObject* FileClass_mode(FileInfo* self);

PyObject* FileClass_write(FileInfo* self, PyObject *args, PyObject *kwds);

PyObject* FileClass_get_mode(FileInfo *self);

PyObject* FileClass_available(FileInfo *self);

PyObject* FileClass_read(FileInfo *self, PyObject *args, PyObject *kwds);

PyObject* FileClass_read_chunk(FileInfo *self, PyObject *args, PyObject *kwds);

PyObject* FileClass_pread(FileInfo *self, PyObject *args, PyObject *kwds);

PyObject* FileClass_pread_chunk(FileInfo *self, PyObject *args, PyObject *kwds);

PyObject* FileClass_seek(FileInfo *self, PyObject *args, PyObject *kwds);

PyObject* FileClass_tell(FileInfo *self, PyObject *args, PyObject *kwds);

PyObject* FileClass_flush(FileInfo *self);

#endif
