#include "hdfs_file.h"
#include "hdfs_utils.h"

using namespace std;

PyObject*
FileClass_new(PyTypeObject *type, PyObject *args, PyObject *kwds)
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
        self->readline_chunk_size=16384;
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
    if (! PyArg_ParseTuple(args, "OO",
            &(self->fs), &(self->file)))
        PyErr_SetString(PyExc_ValueError, "Error parsing arguments");
        return NULL;

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

    static char *kwlist[] = {"size", NULL};
    #ifdef HADOOP_LIBHDFS_V1
    if(!hdfsFileIsOpenForRead(self)){
    #else
    if(!hdfsFileIsOpenForRead(self->file)){
    #endif
        PyErr_SetString(PyExc_IOError, "File is not opened in READ ('r') mode");
        return NULL;
    }

    if (! PyArg_ParseTupleAndKeywords(args, kwds, "i", kwlist, &(size)))
        Py_RETURN_NONE;

    buffer = malloc(sizeof(tSize) * size);

    tSize read = hdfsRead(self->fs, self->file, buffer, size);

    return Py_BuildValue("s#", buffer, read);
}


PyObject* FileClass_read_chunk(FileInfo *self, PyObject *args, PyObject *kwds){

    int chunk_size;
    void* buffer;

    static char *kwlist[] = {"chunk", NULL};

    #ifdef HADOOP_LIBHDFS_V1
    if(!hdfsFileIsOpenForRead(self)){
    #else
    if(!hdfsFileIsOpenForRead(self->file)){
    #endif
        PyErr_SetString(PyExc_IOError, "File is not opened in READ ('r') mode");
        return NULL;
    }

    if (! PyArg_ParseTupleAndKeywords(args, kwds, "s#", kwlist, &(buffer), &chunk_size))
        Py_RETURN_NONE;

    tSize read = hdfsRead(self->fs, self->file, buffer, chunk_size);
    return Py_BuildValue("i", read);
}




PyObject* FileClass_pread(FileInfo *self, PyObject *args, PyObject *kwds){

    tSize position;
    tSize length;

    static char *kwlist[] = {"position", "length", NULL};

    #ifdef HADOOP_LIBHDFS_V1
    if(!hdfsFileIsOpenForRead(self)){
    #else
    if(!hdfsFileIsOpenForRead(self->file)){
    #endif
        PyErr_SetString(PyExc_IOError, "File is not opened in READ ('r') mode");
        return NULL;
    }

    if (! PyArg_ParseTupleAndKeywords(args, kwds, "ii", kwlist, &position, &length))
        Py_RETURN_NONE;

    void* buffer;
    buffer = malloc(sizeof(tSize) * length);
    tSize read = hdfsPread(self->fs, self->file, position, buffer, length);
    return Py_BuildValue("s#", buffer, read);
}


PyObject* FileClass_pread_chunk(FileInfo *self, PyObject *args, PyObject *kwds){

    tSize position, chunk_size;
    void*buffer;

    static char *kwlist[] = {"position", "chunk", NULL};

    #ifdef HADOOP_LIBHDFS_V1
    if(!hdfsFileIsOpenForRead(self)){
    #else
    if(!hdfsFileIsOpenForRead(self->file)){
    #endif
        PyErr_SetString(PyExc_IOError, "File is not opened in READ ('r') mode");
        return NULL;
    }

    if (! PyArg_ParseTupleAndKeywords(args, kwds, "is#", kwlist, &position, &buffer, &chunk_size))
        Py_RETURN_NONE;

    tSize read = hdfsPread(self->fs, self->file, position, buffer, chunk_size);
    return Py_BuildValue("i", read);
}


PyObject* FileClass_seek(FileInfo *self, PyObject *args, PyObject *kwds){

    tSize position;

    static char *kwlist[] = {"position", NULL};

    if (! PyArg_ParseTupleAndKeywords(args, kwds, "i", kwlist, &position))
        Py_RETURN_NONE;

    int result = hdfsSeek(self->fs, self->file, position);
    return Py_BuildValue("i", result);
}



PyObject* FileClass_tell(FileInfo *self, PyObject *args, PyObject *kwds){

    tOffset offset = hdfsTell(self->fs, self->file);
    return Py_BuildValue("i", offset);
}



PyObject* FileClass_write(FileInfo* self, PyObject *args, PyObject *kwds)
{

    char* buffer;
    int buffer_length;
    static char *kwlist[] = {"data", NULL};

    #ifdef HADOOP_LIBHDFS_V1
    if(!hdfsFileIsOpenForWrite(self)){
    #else
    if(!hdfsFileIsOpenForWrite(self->file)){
    #endif
        PyErr_SetString(PyExc_IOError, "File is not opened in WRITE ('w') mode");
        return NULL;
    }

    if (! PyArg_ParseTupleAndKeywords(args, kwds, "s#", kwlist,
            &(buffer), &buffer_length))
        Py_RETURN_NONE;

    int written = hdfsWrite(self->fs, self->file, buffer, buffer_length);
    return Py_BuildValue("i", written);
}



PyObject* FileClass_write_chunk(FileInfo* self, PyObject *args, PyObject *kwds)
{

    char* buffer;
    int buffer_length;
    static char *kwlist[] = {"chunk", NULL};

    #ifdef HADOOP_LIBHDFS_V1
    if(!hdfsFileIsOpenForWrite(self)){
    #else
    if(!hdfsFileIsOpenForWrite(self->file)){
    #endif
        PyErr_SetString(PyExc_IOError, "File is not opened in WRITE ('w') mode");
        return NULL;
    }

    if (! PyArg_ParseTupleAndKeywords(args, kwds, "s#", kwlist,
            &(buffer), &buffer_length))
        Py_RETURN_NONE;

    int written = hdfsWrite(self->fs, self->file, buffer, buffer_length);
    return Py_BuildValue("i", written);
}



PyObject* FileClass_flush(FileInfo *self){
    int result = hdfsFlush(self->fs, self->file);
    return Py_BuildValue("i", result);
}

