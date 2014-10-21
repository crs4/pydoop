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
