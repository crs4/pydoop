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
#ifndef PYDOOP_SERIALIZE_SERIALIZATION_HH
#define PYDOOP_SERIALIZE_SERIALIZATION_HH

#include <Python.h>
#include <string>
#include "SerialUtils.hh"

namespace hu = HadoopUtils;

PyObject* serialize_int(hu::OutStream* stream, PyObject* code);
PyObject* deserialize_int(hu::InStream* stream);

PyObject* serialize(hu::OutStream* stream, const std::string& srule,
                    const PyObject* data);

PyObject* deserialize(hu::InStream* stream, const std::string& srule);


#endif // PYDOOP_SERIALIZE_SERIALIZATION_HH
