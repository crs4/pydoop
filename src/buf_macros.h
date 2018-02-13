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

#ifndef PYDOOP_BUF_MACROS
#define PYDOOP_BUF_MACROS 1


#if IS_PY3K
#define _PyBuf_FromStringAndSize(s,nbytes) PyBytes_FromStringAndSize(s, nbytes)
#define _PyBuf_AS_STRING(b) PyBytes_AS_STRING(b)
#define _PyBuf_Resize(b, n) _PyBytes_Resize(b, n)
#define _PyBuf_FromString(x) PyBytes_FromString(x)
#else
#define _PyBuf_FromStringAndSize(s,nbytes) PyString_FromStringAndSize(s, nbytes)
#define _PyBuf_AS_STRING(b) PyString_AS_STRING(b)
#define _PyBuf_Resize(b, n) _PyString_Resize(b, n)
#define _PyBuf_FromString(x) PyString_FromString(x)
#endif

#endif /* PYDOOP_BUF_MACROS */
