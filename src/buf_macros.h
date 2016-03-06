#ifndef PYDOOP_BUF_MACROS
#define PYDOOP_BUF_MACROS 1


#if PY_MAJOR_VERSION >= 3
#define IS_PY3K 1
#endif


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
