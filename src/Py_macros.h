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
#ifndef PYDOOP_PY_MACROS
#define PYDOOP_PY_MACROS 1


// FIXME: PyBytes should be ok in py2.7 too.

#if IS_PY3K
#define PyInt_Check PyLong_Check
#define PyInt_AsLong PyLong_AsLong
#define PyInt_AsSsize_t PyLong_AsSsize_t
#define PyString_Check PyBytes_Check
#define PyString_AsString PyBytes_AsString
#else

#endif


#endif  // PYDOOP_PY_MACROS
