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

#include <Python.h>

#if PY_MAJOR_VERSION >= 3
#define IS_PY3K 1
#endif

#include <string>
#include <map>
#include <utility>  // std::pair support
#include <iostream>
#include "SerialUtils.hh"
#include <errno.h>

#include "../buf_macros.h"
#include "../Py_macros.h"


static char* module__name__ = "sercore";
static char* module__doc__ = "serialization routine implementation";


/*
  Encoding/Decoding formats:

  s: string (a buffer in py3)
  i: int32
  L: int64
  A: a list of strings (!)

 */

namespace hu = HadoopUtils;

static PyObject *ProtocolCodecError;

typedef std::map<int, std::pair<std::string, std::string> > cmd_decoding_t;
typedef std::map<std::string, std::pair<int, std::string> > cmd_encoding_t;
typedef cmd_encoding_t::mapped_type args_encoding_t;
typedef cmd_decoding_t::mapped_type args_decoding_t;

// FIXME: implement this thing as a "proper python" class 

class ProtocolCodec {
private:
  
  inline PyObject* handle_error(hu::Error& e) {
    if (e.getMessage().find("end of file") != std::string::npos) {
      PyErr_SetObject(PyExc_EOFError, Py_None);
    } else {
      PyErr_SetString(ProtocolCodecError, e.getMessage().c_str());        
    }
    return NULL;
  }

public:

  ProtocolCodec() {}

  PyObject* add_rule(int code, const std::string& name, 
                     const std::string& enc_format) {
    _decoding_rules[code] = std::make_pair(name, enc_format);
    _encoding_rules[name] = std::make_pair(code, enc_format);
    Py_INCREF(Py_None);
    return Py_None;
  }

  inline PyObject* encode_cmd_to_stream(std::string cmd, PyObject* args, 
                                        hu::OutStream& stream) {
    if (_encoding_rules.find(cmd) == _encoding_rules.end()) {
      PyErr_SetString(ProtocolCodecError, "Unknown command code.");
      return NULL;
    }
    if (_encoding_rules.find(cmd) == _encoding_rules.end()) {
      PyErr_SetString(ProtocolCodecError, "Unknown command code.");
      return NULL;
    }
    int code = _encoding_rules.at(cmd).first;
    std::string encoding = _encoding_rules.at(cmd).second;
    if (encoding.size() != (size_t)PyTuple_Size(args)) {
      PyErr_SetString(ProtocolCodecError, 
                      "Wrong number of arguments for the formatting rule.");
      return NULL;
    }
    PyObject* res; 
    try {
      Py_BEGIN_ALLOW_THREADS;
      serializeInt(code, stream);
      Py_END_ALLOW_THREADS;
      res = serialize_to_stream(encoding, args, stream);
    } catch (hu::Error& e) {
      return handle_error(e);
    }
    return res;
  }

  inline PyObject* decode_cmd_from_stream(hu::InStream& stream) {
    int code;
    Py_BEGIN_ALLOW_THREADS;
    try {
      code = deserializeInt(stream);
    } catch (hu::Error& e) {
      Py_BLOCK_THREADS;
      return handle_error(e);
    }
    Py_END_ALLOW_THREADS;
    if (_decoding_rules.find(code) == _decoding_rules.end()) {
        PyErr_SetString(ProtocolCodecError, "Unknown command code.");
        return NULL;
    }
    PyObject* args = deserialize_from_stream(_decoding_rules.at(code).second, 
                                             stream);
    if (args == NULL) {
      return NULL;
    }
    PyObject* res = PyTuple_New(2);
    const std::string& name = _decoding_rules.at(code).first;
    PyTuple_SET_ITEM(res, 0, PyUnicode_FromString(name.c_str()));
    PyTuple_SET_ITEM(res, 1, args);
    return res;
  }

  inline PyObject* serialize_to_stream(const std::string& enc_rule, PyObject* args,
                                       hu::OutStream& stream) {
    for(std::size_t i = 0; i < enc_rule.size(); ++i) {
      PyObject* v = serialize_item(enc_rule[i], PyTuple_GET_ITEM(args, i), stream);
      if (v == NULL) {
        return NULL;
      }
    }
    Py_INCREF(Py_None);
    return Py_None;
  }

  inline PyObject* deserialize_from_stream(const std::string& enc_rule, 
                                           hu::InStream& stream) {
    PyObject* res = PyTuple_New(enc_rule.size());
    for(std::size_t i = 0; i < enc_rule.size(); ++i) {
      PyObject* v = deserialize_item(enc_rule[i], stream);
      if (v == NULL) {
        return NULL;
      }
      PyTuple_SET_ITEM(res, i, v);
    }
    return res;
  }
  inline PyObject* deserialize_from_buffer(const std::string& enc_rule, 
                                           const std::string& buffer) {
    hu::StringInStream sis(buffer);
    return deserialize_from_stream(enc_rule, sis);
  }

  inline PyObject* serialize_item(char code, PyObject* o,
                                  hu::OutStream& stream) {
    try {
      switch(code) {
      case 's': {
#if IS_PY3K
        // FIXME I think that this is actually python2.7 compatible as it is...
        Py_buffer buffer;
        if (PyObject_GetBuffer(o, &buffer, PyBUF_SIMPLE) < 0) {
          PyErr_SetString(PyExc_TypeError,
                          "Argument is not accessible as a Python buffer");
        }
        else {
          std::string s((char *) buffer.buf, buffer.len);
          Py_BEGIN_ALLOW_THREADS;
          serializeString(s, stream);
          Py_END_ALLOW_THREADS;
        }
#else
        Py_ssize_t size;
        char *ptr;
        if (PyString_AsStringAndSize(o, &ptr, &size) == -1) {
          return NULL;
        }
        std::string s(ptr, size);
        Py_BEGIN_ALLOW_THREADS;
        serializeString(s, stream);
        Py_END_ALLOW_THREADS;
#endif
        return o; // everything is ok.
      }
      case 'i': {
#if IS_PY3K
        long v = PyLong_AsLong(o);        
#else
        long v = PyInt_AsLong(o);
#endif
        if (v == -1 && PyErr_Occurred()) {
          return NULL;
        }
        Py_BEGIN_ALLOW_THREADS;
        serializeInt(v, stream);
        Py_END_ALLOW_THREADS;
        return o;
      }
      case 'L': {
        long long v = PyLong_AsLongLong(o);
        if (v == -1 && PyErr_Occurred()) {
          return NULL;
        }
        Py_BEGIN_ALLOW_THREADS;
        serializeLong(v, stream);
        Py_END_ALLOW_THREADS;
        return o;
      }
      case 'f': {
        float v = PyFloat_AsDouble(o);
        if (v == -1.0 && PyErr_Occurred()) {
          return NULL;
        }
        Py_BEGIN_ALLOW_THREADS;
        serializeFloat(v, stream);
        Py_END_ALLOW_THREADS;
        return o;
      }
      case 'A': {
        if (!PyTuple_Check(o)) {
          PyErr_SetString(ProtocolCodecError, "A argument should be a tuple.");
          return NULL;
        }
        Py_ssize_t n = PyTuple_GET_SIZE(o);
        Py_BEGIN_ALLOW_THREADS;
        serializeInt(n, stream);
        Py_END_ALLOW_THREADS;
        for(Py_ssize_t i = 0; i < n; ++i){
          serialize_item('s', PyTuple_GET_ITEM(o, i), stream);
        }
        return o;
      }
      default:
        PyErr_SetString(ProtocolCodecError, "Unknown decoding code.");
        return NULL;
      } 

    } catch (hu::Error& e) {
      return handle_error(e);
    }
  }
  inline PyObject* deserialize_item(char code, hu::InStream& stream) {
    try {
      switch(code) {
      case 's': {
        Py_BEGIN_ALLOW_THREADS;
        deserializeString(_buffer, stream);
        Py_END_ALLOW_THREADS;
        return _PyBuf_FromStringAndSize(_buffer.c_str(), _buffer.size());
      }
      case 'i': {
        long v;
        Py_BEGIN_ALLOW_THREADS;
        v = deserializeInt(stream);
        Py_END_ALLOW_THREADS;
        return PyLong_FromLong(v);
      }
      case 'L': {
        long long v;
        Py_BEGIN_ALLOW_THREADS;        
        v = deserializeLong(stream);
        Py_END_ALLOW_THREADS;      
        return PyLong_FromLongLong(v);
      }
      case 'f': {
        float v;
        Py_BEGIN_ALLOW_THREADS;
        deserializeFloat(v, stream);
        Py_END_ALLOW_THREADS;
        return PyFloat_FromDouble((double)v);
      }
      case 'A': {
        Py_ssize_t n;
        Py_BEGIN_ALLOW_THREADS;        
        n = deserializeInt(stream);
        Py_END_ALLOW_THREADS;
        PyObject* res = PyTuple_New(n);        
        for(Py_ssize_t i = 0; i < n; ++i){
          PyTuple_SET_ITEM(res, i, deserialize_item('s', stream));
        }
        return res;
      }
      default:
        PyErr_SetString(ProtocolCodecError, "Unknown decoding code.");
        return NULL;
      } 
    } catch (hu::Error& e) {
      return handle_error(e);
    }
  }

private:
  std::string _buffer;
  cmd_encoding_t _encoding_rules;
  cmd_decoding_t _decoding_rules;
};

/*
struct A{
  static const cmd_encoding_t create_map() {
    cmd_encoding_t m;
    m[MAP_ITEM] = std::string("ss");
    m[CLOSE] = std::string("");
    return m;
  }
  static const cmd_encoding_t _map;
};

const cmd_encoding_t A::_map = A::create_map();

static ProtocolCodec decoder(A::_map);
*/
static ProtocolCodec codec;

static PyObject *
codec_decode_command(PyObject *self, PyObject *args) {

  PyObject *po = PyTuple_GetItem(args, 0);

  hu::FileInStream stream;  

#if IS_PY3K
  int fd = PyObject_AsFileDescriptor(po);
  if (fd < 0) {
    PyErr_SetString(ProtocolCodecError, "First argument should be a file.");
    return NULL;
  }
  stream.open(fdopen(fd, "r"));  
#else
  if (!PyFile_Check(po)) {
    PyErr_SetString(ProtocolCodecError, "First argument should be a file.");
    return NULL;
  }
  stream.open(PyFile_AsFILE(po));
#endif

  PyObject* res; 
  if (PyTuple_Size(args) == 1) {
    res = codec.decode_cmd_from_stream(stream);
  } else {
    PyObject *pn = PyTuple_GetItem(args, 1);
    Py_ssize_t N = PyInt_AsSsize_t(pn);

    res = PyTuple_New(N);
    for(Py_ssize_t i = 0;  i < N ; ++i) {
      PyObject* cmd = codec.decode_cmd_from_stream(stream);
      if (cmd == NULL) {
        if (PyErr_Occurred() == PyExc_EOFError) {
          _PyTuple_Resize(&res, i);
          PyErr_Clear();
          break;
        }
        return NULL;
      }
      PyTuple_SET_ITEM(res, i, cmd);
    }
  }
  return res;
}

static PyObject *
codec_encode_command(PyObject *self, PyObject *args) {
  PyObject *po = PyTuple_GET_ITEM(args, 0);

  hu::FileOutStream stream;
  
#if IS_PY3K
  int fd = PyObject_AsFileDescriptor(po);
  if (fd < 0) {
    PyErr_SetString(ProtocolCodecError, "First argument should be a file.");
    return NULL;
  }
  stream.open(fdopen(fd, "w"));  
#else
  if (!PyFile_Check(po)) {
    PyErr_SetString(ProtocolCodecError, "First argument should be a file.");
    return NULL;
  }
  stream.open(PyFile_AsFILE(po));
#endif
  
  PyObject *pcmd = PyTuple_GET_ITEM(args, 1);
  if (!PyString_Check(pcmd)) {
    PyErr_SetString(ProtocolCodecError, "Second argument should be a cmd name.");
    return NULL;
  }
  std::string cmd(PyString_AsString(pcmd));
  PyObject* res = codec.encode_cmd_to_stream(cmd, PyTuple_GET_ITEM(args, 2),
                                             stream);  
  return res;
}

/*
  codec.deserialize_from_stream(stream, enc_format) -> (args_tuple)
 */
static PyObject *
codec_deserialize_from_stream(PyObject *self, PyObject *args) {
  PyErr_SetString(ProtocolCodecError, "NOT IMPLEMENTED");
  return NULL;
}

/*
  codec.serialize_to_stream(enc_format, args, stream)
 */
static PyObject *
codec_serialize_to_stream(PyObject *self, PyObject *args) {
  PyErr_SetString(ProtocolCodecError, "NOT IMPLEMENTED");
  return NULL;
}

/*
  codec.deserialize_from_buffer(buffer, enc_format) -> (args_tuple)
 */
static PyObject *
codec_deserialize_from_buffer(PyObject *self, PyObject *args) {
  PyErr_SetString(ProtocolCodecError, "NOT IMPLEMENTED");
  return NULL;
}

/*
  codec.serialize_to_buffer(enc_format, args, buffer)
 */
static PyObject *
codec_serialize_to_buffer(PyObject *self, PyObject *args) {
  PyErr_SetString(ProtocolCodecError, "NOT IMPLEMENTED");
  return NULL;
}

static PyObject *
codec_add_rule(PyObject *self, PyObject *args) {
  int code;
  const char* name;
  const char* enc_format;
  if (!PyArg_ParseTuple(args, "iss", &code, &name, &enc_format))
    return NULL;
  return codec.add_rule(code, std::string(name), std::string(enc_format));
}

static PyObject *
util_fdopen(PyObject *self, PyObject *args) {
  int fd;
  char *mode;
  int bufsize = -1;
  if (!PyArg_ParseTuple(args, "isi", &fd, &mode, &bufsize))
    return NULL;
  // FIXME: NO ARGS CHECKING!
#if IS_PY3K
  return PyFile_FromFd(fd, "<fdopen>", mode, bufsize, NULL, NULL, NULL, true);
#else
  FILE *fp = fdopen(fd, mode);
  char *buffer = new char[bufsize];
  int setbuf = setvbuf(fp, buffer, _IOFBF, bufsize);
  if (setbuf != 0) {
    delete [] buffer;
    std::string msg = std::string("problems with setvbuf:") + strerror(errno);
    PyErr_SetString(ProtocolCodecError, msg.c_str());
    return NULL;
  }
  return PyFile_FromFile(fp, "<fdopen>", mode, fclose);
#endif
}

static PyMethodDef module_methods[] = {
  {"add_rule", codec_add_rule, METH_VARARGS,
   "Add protocol command decoding rule"},
  {"decode_command", codec_decode_command, METH_VARARGS,
   "Decode command from stream"},
  {"encode_command", codec_encode_command, METH_VARARGS,
   "Encode command to stream"},
  {"serialize_to_stream", codec_serialize_to_stream, METH_VARARGS,
   "Serialize a list of objects to a stream"},
  {"deserialize_from_stream", codec_deserialize_from_stream, METH_VARARGS,
   "Deserialize a list of objects from a stream"},
  {"serialize_to_buffer", codec_serialize_to_buffer, METH_VARARGS,
   "Serialize a list of objects to a buffer"},
  {"deserialize_from_buffer", codec_deserialize_from_buffer, METH_VARARGS,
   "Deserialize a list of objects from a buffer"},
  {"fdopen", util_fdopen, METH_VARARGS, 
   "Return an open file object connected to a file descriptor"},
  {NULL, NULL, 0, NULL}        /* Sentinel */
};


#ifndef PyMODINIT_FUNC	/* declarations for DLL import/export */
#define PyMODINIT_FUNC void
#endif

#if IS_PY3K
static struct PyModuleDef module_def = {
  PyModuleDef_HEAD_INIT,
  module__name__, /* m_name */
  module__doc__,  /* m_doc */
  -1,                  /* m_size */
  module_methods,    /* m_methods */
  NULL,                /* m_reload */
  NULL,                /* m_traverse */
  NULL,                /* m_clear */
  NULL,                /* m_free */
};
#endif


#if IS_PY3K

PyMODINIT_FUNC
PyInit_sercore(void)
{
  PyObject* m;
  m = PyModule_Create(&module_def);
  if (m == NULL)
    return NULL;

  ProtocolCodecError = PyErr_NewException("ProtocolCodec.error", NULL, NULL);
  Py_INCREF(ProtocolCodecError);
  PyModule_AddObject(m, "error", ProtocolCodecError);
  return m;
}

#else

PyMODINIT_FUNC
initsercore(void)
{
  PyObject* m;

  if (PyType_Ready(&FsType) < 0)
    return;
  if (PyType_Ready(&FileType) < 0)
    return;
  m = Py_InitModule3(module__name__, module_methods,
                     module__doc__);
  if (m == NULL)
    return;

  ProtocolCodecError = PyErr_NewException("ProtocolCodec.error", NULL, NULL);
  Py_INCREF(ProtocolCodecError);
  PyModule_AddObject(m, "error", ProtocolCodecError);
}
#endif


