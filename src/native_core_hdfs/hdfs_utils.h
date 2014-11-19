/* BEGIN_COPYRIGHT
 *
 * Copyright 2009-2014 CRS4.
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

#ifndef __hdfs_utils_H_
#define __hdfs_utils_H_

#include <Python.h>
//#include <errno.h>
#include <typeinfo>


namespace hdfs4python {


    class Utils {

    public:

        static char* getObjectAsUTF8String(PyObject *obj);

        static char *getUnicodeAsUTF8String(PyUnicodeObject *obj);

        static bool parsePathStringArgAsUTF8(PyObject * args, PyObject *kwds, char *arg_name, char **arg_value);

        //static bool parsePathStringAndArgs(PyObject * args, PyObject *kwds, char * format, char ** kwlist, char **path, ...);
    };
}

#endif //__hdfs_utils_H_
