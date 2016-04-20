#ifndef PYDOOP_PY_MACROS
#define PYDOOP_PY_MACROS 1


#if IS_PY3K
#define PyInt_Check PyLong_Check
#define PyInt_AsLong PyLong_AsLong
#define PyInt_AsSsize_t PyLong_AsSsize_t
#define PyString_Check PyBytes_Check
#define PyString_AsString PyBytes_AsString
#else

#endif


#endif  // PYDOOP_PY_MACROS
