// BEGIN_COPYRIGHT
//
// Copyright 2009-2018 CRS4.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy
// of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.
//
// END_COPYRIGHT

#pragma once

#include <Python.h>
#include <memory>
#include <string>
#include "HadoopUtils/SerialUtils.hh"

typedef struct {
    PyObject_HEAD
    std::shared_ptr<HadoopUtils::FileInStream> stream;
} FileInStreamObj;

typedef struct {
    PyObject_HEAD
    std::shared_ptr<HadoopUtils::FileOutStream> stream;
} FileOutStreamObj;

typedef struct {
    PyObject_HEAD
    std::shared_ptr<std::string> data;
    std::shared_ptr<HadoopUtils::StringInStream> stream;
} StringInStreamObj;

extern PyTypeObject FileInStreamType;
extern PyTypeObject FileOutStreamType;
extern PyTypeObject StringInStreamType;
