#include <Python.h>

#include <string>
#include <map>
#include <utility>  // std::pair support
#include <iostream>
#include "SerialUtils.hh"


/*
  Encoding/Decoding formats:

  s: string
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
    if (encoding.size() != PyTuple_Size(args)) {
      PyErr_SetString(ProtocolCodecError, 
                      "Wrong number of arguments for the formatting rule.");
      return NULL;
    }
    PyObject* res; 
    try {
      serializeInt(code, stream);
      res = serialize_to_stream(encoding, args, stream);
    } catch (hu::Error& e) {
      return handle_error(e);
    }
    return res;
  }

  inline PyObject* decode_cmd_from_stream(hu::InStream& stream) {
    int code;
    try {
      code = deserializeInt(stream);
    } catch (hu::Error& e) {
      return handle_error(e);
    }
    if (_decoding_rules.find(code) == _decoding_rules.end()) {
        PyErr_SetString(ProtocolCodecError, "Unknown command code.");
        return NULL;
    }
    PyObject* args = deserialize_from_stream(_decoding_rules.at(code).second, 
                                             stream);
    if (args == NULL) {
      return NULL;
    }
#if 1
    PyObject* res = PyTuple_New(2);
    const std::string& name = _decoding_rules.at(code).first;
    PyTuple_SET_ITEM(res, 0, 
                     PyString_FromStringAndSize(name.c_str(), name.size()));
#else
    PyTuple_SET_ITEM(res, 0, PyInt_FromLong(code));
#endif
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
                                           const std::string buffer) {
    hu::StringInStream sis(buffer);
    return deserialize_from_stream(enc_rule, sis);
  }

  inline PyObject* serialize_item(char code, PyObject* o, hu::OutStream& stream) {
    try {
      switch(code) {
      case 's': {
        Py_ssize_t size;
        char *ptr;
        if (PyString_AsStringAndSize(o, &ptr, &size) == -1) {
          return NULL;
        }
        std::string s(ptr, size);
        serializeString(s, stream);
        return o; // everything is ok.
      }
      case 'i': {
        long v = PyInt_AsLong(o);
        if (v == -1 && PyErr_Occurred()) {
          return NULL;
        }
        serializeInt(v, stream);
        return o;
      }
      case 'L': {
        long long v = PyLong_AsLongLong(o);
        if (v == -1 && PyErr_Occurred()) {
          return NULL;
        }
        serializeLong(v, stream);
        return o;
      }
      case 'A': {
        if (!PyTuple_Check(o)) {
          PyErr_SetString(ProtocolCodecError, "A argument should be a tuple.");
          return NULL;
        }
        Py_ssize_t n = PyTuple_GET_SIZE(o);
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
      case 's':
        deserializeString(_buffer, stream);
        std::cerr << "Deserializing string: "<< _buffer << std::endl;
        return PyString_FromStringAndSize(_buffer.c_str(), _buffer.size());
      case 'i':
        return PyInt_FromLong(deserializeInt(stream));
      case 'L':
        return PyLong_FromLongLong(deserializeLong(stream));
      case 'A': {
        Py_ssize_t n = deserializeInt(stream);
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
  if (!PyFile_Check(po)) {
    PyErr_SetString(ProtocolCodecError, "First argument should be  a file object.");
    return NULL;
  }

  hu::FileInStream stream;
  stream.open(PyFile_AsFile(po));

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
  if (!PyFile_Check(po)) {
    PyErr_SetString(ProtocolCodecError, "First argument should be  a file object.");
    return NULL;
  }
  PyObject *pcmd = PyTuple_GET_ITEM(args, 1);
  if (!PyString_Check(pcmd)) {
    PyErr_SetString(ProtocolCodecError, "Second argument should be a cmd name.");
    return NULL;
  }
  hu::FileOutStream stream;
  stream.open(PyFile_AsFile(po));
  std::string cmd(PyString_AsString(pcmd));
  PyObject* res = codec.encode_cmd_to_stream(cmd, PyTuple_GET_ITEM(args, 2), stream);  
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

static PyMethodDef ProtocolCodecMethods[] = {
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
  {NULL, NULL, 0, NULL}        /* Sentinel */
};

PyMODINIT_FUNC
initpydoop_sercore(void)
{
  PyObject *m;
  m = Py_InitModule("pydoop_sercore", ProtocolCodecMethods);
  if (m == NULL) 
    return;
  ProtocolCodecError = PyErr_NewException("ProtocolCodec.error", NULL, NULL);
  Py_INCREF(ProtocolCodecError);
  PyModule_AddObject(m, "error", ProtocolCodecError);
}
