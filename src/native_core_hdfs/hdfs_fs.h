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
#include <structmember.h>

#include "../py3k_compat.h"


#define MODE_READ "r"
#define MODE_WRITE "w"
#define MODE_APPEND "a"


typedef struct {
    PyObject_HEAD
    char *host;
    int port;
    char *user;
    char *group;
    hdfsFS _fs;
} FsInfo;


PyObject* FsClass_new(PyTypeObject* type, PyObject *args, PyObject *kwds);

void FsClass_dealloc(FsInfo* self);

int FsClass_init(FsInfo* self, PyObject *args, PyObject *kwds);

PyObject* FsClass_close(FsInfo* self);

PyObject* FsClass_get_working_directory(FsInfo* self);

PyObject* FsClass_get_default_block_size(FsInfo* self);

PyObject* FsClass_get_path_info(FsInfo* self, PyObject *args, PyObject *kwds);

PyObject* FsClass_get_hosts(FsInfo* self, PyObject *args, PyObject *kwds);

PyObject* FsClass_get_used(FsInfo* self);

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

#endif
