#include "hdfs_file.h"
#include "hdfs_utils.h"

using namespace std;

PyObject* FileClass_new(PyTypeObject *type, PyObject *args, PyObject *kwds)
{
    FileInfo *self;

    self = (FileInfo *)type->tp_alloc(type, 0);
    if (self != NULL) {
        self->fs = NULL;
        self->file = NULL;
        self->flags = 0;
        self->buff_size = 0;
        self->replication = 1;
        self->blocksize = 0;
        self->readline_chunk_size = 16 * 1024; // 16 KB
#ifdef HADOOP_LIBHDFS_V1
        self->stream_type = 0;
#endif
    }
    return (PyObject *)self;
}


#ifdef HADOOP_LIBHDFS_V1

bool hdfsFileIsOpenForWrite(FileInfo *f){
    return f->stream_type == OUTPUT;
}


bool hdfsFileIsOpenForRead(FileInfo *f){
    return f->stream_type == INPUT;
}

#endif

void FileClass_dealloc(FileInfo* self)
{
    self->file = NULL;
    self->ob_type->tp_free((PyObject*)self);
}


int FileClass_init(FileInfo *self, PyObject *args, PyObject *kwds)
{
    if (! PyArg_ParseTuple(args, "OO", &(self->fs), &(self->file))) {
        return -1;
    }

    return 0;
}


int FileClass_init_internal(FileInfo *self, hdfsFS fs, hdfsFile file)
{
    self->fs = fs;
    self->file = file;

    return 0;
}


PyObject* FileClass_close(FileInfo* self){
    int result = hdfsCloseFile(self->fs, self->file);
    return PyBool_FromLong(result >= 0 ? 1 : 0);
}


PyObject* FileClass_mode(FileInfo* self){
    return FileClass_get_mode(self);
}


PyObject* FileClass_get_mode(FileInfo *self){
    return Py_BuildValue("i", self->flags);
}



PyObject* FileClass_available(FileInfo *self){
    int available = hdfsAvailable(self->fs, self->file);
    return Py_BuildValue("i", available);
}



PyObject* FileClass_read(FileInfo *self, PyObject *args, PyObject *kwds){

    void* buffer;
    int size;

    #ifdef HADOOP_LIBHDFS_V1
    if(!hdfsFileIsOpenForRead(self)){
    #else
    if(!hdfsFileIsOpenForRead(self->file)){
    #endif
        PyErr_SetString(PyExc_IOError, "File is not opened in READ ('r') mode");
        return NULL;
    }

    if (! PyArg_ParseTuple(args, "i", &(size)))
        Py_RETURN_NONE;

    buffer = PyMem_Malloc(size);

    tSize read = hdfsRead(self->fs, self->file, buffer, size);
    PyObject* res = Py_BuildValue("s#", buffer, read);
    
    PyMem_Free(buffer);
    return res;
}


PyObject* FileClass_read_chunk(FileInfo *self, PyObject *args, PyObject *kwds){

    int chunk_size;
    void* buffer;

    #ifdef HADOOP_LIBHDFS_V1
    if(!hdfsFileIsOpenForRead(self)){
    #else
    if(!hdfsFileIsOpenForRead(self->file)){
    #endif
        PyErr_SetString(PyExc_IOError, "File is not opened in READ ('r') mode");
        return NULL;
    }

    if (! PyArg_ParseTuple(args, "s#",  &buffer, &chunk_size))
        Py_RETURN_NONE;

    tSize read = hdfsRead(self->fs, self->file, buffer, chunk_size);
    return Py_BuildValue("i", read);
}




PyObject* FileClass_pread(FileInfo *self, PyObject *args, PyObject *kwds){

    tOffset position;
    tSize length;

    #ifdef HADOOP_LIBHDFS_V1
    if(!hdfsFileIsOpenForRead(self)){
    #else
    if(!hdfsFileIsOpenForRead(self->file)){
    #endif
        PyErr_SetString(PyExc_IOError, "File is not opened in READ ('r') mode");
        return NULL;
    }

    if (! PyArg_ParseTuple(args, "Li", &position, &length))
        Py_RETURN_NONE;

    void* buffer;
    buffer = PyMem_Malloc(length);
    tSize read = hdfsPread(self->fs, self->file, position, buffer, length);
    PyObject* res = Py_BuildValue("s#", buffer, read);
    PyMem_Free(buffer);
    return res;
}


PyObject* FileClass_pread_chunk(FileInfo *self, PyObject *args, PyObject *kwds){

    tOffset position;
    tSize chunk_size;
    void *buffer;


    #ifdef HADOOP_LIBHDFS_V1
    if(!hdfsFileIsOpenForRead(self)){
    #else
    if(!hdfsFileIsOpenForRead(self->file)){
    #endif
        PyErr_SetString(PyExc_IOError, "File is not opened in READ ('r') mode");
        return NULL;
    }

    if (! PyArg_ParseTuple(args, "Ls#", &position, &buffer, &chunk_size))
        Py_RETURN_NONE;

    tSize read = hdfsPread(self->fs, self->file, position, buffer, chunk_size);
    return Py_BuildValue("i", read);
}


PyObject* FileClass_seek(FileInfo *self, PyObject *args, PyObject *kwds){

    tOffset position;

    if (! PyArg_ParseTuple(args, "L", &position))
        Py_RETURN_NONE;

    int result = hdfsSeek(self->fs, self->file, position);
    return Py_BuildValue("i", result);
}



PyObject* FileClass_tell(FileInfo *self, PyObject *args, PyObject *kwds){

    tOffset offset = hdfsTell(self->fs, self->file);
    return Py_BuildValue("L", offset);
}



PyObject* FileClass_write(FileInfo* self, PyObject *args, PyObject *kwds)
{

    char* buffer;
    int buffer_length;

    #ifdef HADOOP_LIBHDFS_V1
    if(!hdfsFileIsOpenForWrite(self)){
    #else
    if(!hdfsFileIsOpenForWrite(self->file)){
    #endif
        PyErr_SetString(PyExc_IOError, "File is not opened in WRITE ('w') mode");
        return NULL;
    }

    if (! PyArg_ParseTuple(args, "s#",  &buffer, &buffer_length))
        Py_RETURN_NONE;

    int written = hdfsWrite(self->fs, self->file, buffer, buffer_length);
    return Py_BuildValue("i", written);
}



PyObject* FileClass_write_chunk(FileInfo* self, PyObject *args, PyObject *kwds)
{

    char* buffer;
    int buffer_length;

    #ifdef HADOOP_LIBHDFS_V1
    if(!hdfsFileIsOpenForWrite(self)){
    #else
    if(!hdfsFileIsOpenForWrite(self->file)){
    #endif
        PyErr_SetString(PyExc_IOError, "File is not opened in WRITE ('w') mode");
        return NULL;
    }

    if (! PyArg_ParseTuple(args, "s#", &buffer, &buffer_length))
        Py_RETURN_NONE;

    int written = hdfsWrite(self->fs, self->file, buffer, buffer_length);
    return Py_BuildValue("i", written);
}


PyObject* FileClass_flush(FileInfo *self){
    int result = hdfsFlush(self->fs, self->file);
    return Py_BuildValue("i", result);
}

