#include "hdfs_fs.h"
#include "hdfs_file.h"
#include "hdfs_utils.h"

#include <sstream>
#include <hdfs.h>
#include <unicodeobject.h>
#include <errno.h>

#define MAX_WD_BUFFSIZE 2048

#define str_empty(s) ((s) == NULL || strnlen((s), 1) == 0)

using namespace hdfs4python;

PyObject*
FsClass_new(PyTypeObject *type, PyObject *args, PyObject *kwds)
{
    FsInfo *self;

    self = (FsInfo *)type->tp_alloc(type, 0);
    if (self != NULL) {

        self->host = NULL;
        self->port = 0;

        self->user = NULL;
        self->group = NULL;

        self->_fs = NULL;
    }

    return (PyObject *)self;
}


void
FsClass_dealloc(FsInfo* self)
{
    self->ob_type->tp_free((PyObject*)self);
}


int
FsClass_init(FsInfo *self, PyObject *args, PyObject *kwds)
{

    if (! PyArg_ParseTuple(args, "z|izz",
            &(self->host), &(self->port),
            &(self->user), &(self->group)))
        return -1;

    if (str_empty(self->host))
        self->host = NULL;

    if (str_empty(self->user))
        self->user = NULL;

    if (str_empty(self->group))
        self->group = NULL;

    if (self->user != NULL && strnlen(self->user, 1) != 0 ) {
        self->_fs = hdfsConnectAsUser(self->host, self->port, self->user);

    } else {
        self->_fs = hdfsConnect(self->host, self->port);
    }

    if (!self->_fs) {
        PyErr_SetFromErrno(PyExc_RuntimeError);
        return -1;
    }

    return 0;
}


PyObject* FsClass_close(FsInfo* self)
{
    hdfsDisconnect(self->_fs);
    Py_RETURN_NONE;
}


PyObject* FsClass_working_directory(FsInfo* self) {
    return FsClass_get_working_directory(self);
}


PyObject* FsClass_get_working_directory(FsInfo* self) {

    const size_t bufferSize = MAX_WD_BUFFSIZE;
    char *buffer = (char*)PyMem_Malloc(bufferSize);
    if (!buffer)
        return PyErr_NoMemory();

    if (hdfsGetWorkingDirectory(self->_fs, buffer, bufferSize) == NULL) {
        PyErr_SetString(PyExc_RuntimeError, "Cannot get working directory.");
        PyMem_Free(buffer);
        return NULL;
    }

    PyObject* result = PyUnicode_FromString(buffer);
    PyMem_Free(buffer);
    if (!result)
        return PyErr_NoMemory();

    return result;
}

PyObject* FsClass_path_info(FsInfo* self, PyObject *args, PyObject *kwds) {
    return FsClass_get_path_info(self, args, kwds);
}

PyObject* FsClass_get_path_info(FsInfo* self, PyObject *args, PyObject *kwds) {

    PyObject* opath;
    const char* path;

    if (!PyArg_ParseTuple(args, "O",  &opath))  {
        PyErr_SetString(PyExc_ValueError, "Unable to parse the arguments.");
        return NULL;
    }

    path = Utils::getObjectAsUTF8String(opath);
    if (path == NULL) {
        PyErr_SetString(PyExc_ValueError, "Unable to parse the path.");
        return NULL;
    }

    hdfsFileInfo* info = hdfsGetPathInfo(self->_fs, (const char *) path);
    if (info == NULL) {
        PyErr_SetString(PyExc_IOError, "File not found");
        return NULL;
    }

    PyObject* retval =
        Py_BuildValue("{s:O,s:s,s:s,s:i,s:i,s:h,s:s,s:h,s:i,s:O,s:L}",
            "name", PyUnicode_FromString(info->mName),
            "kind", info->mKind == kObjectKindDirectory ? "directory" : "file",
            "group", info->mGroup,
            "last_mod", info->mLastMod,
            "last_access", info->mLastAccess,
            "replication", info->mReplication,
            "owner", info->mOwner,
            "permissions", info->mPermissions,
            "block_size", info->mBlockSize,
            "path", PyUnicode_FromString(info->mName),
            "size", info->mSize
    );
    hdfsFreeFileInfo(info, 1);

    return retval;
}


PyObject* FsClass_get_hosts(FsInfo* self, PyObject *args, PyObject *kwds) {

    Py_ssize_t start, length;
    const char* path;
    PyObject* opath;

    if (!PyArg_ParseTuple(args, "Onn", &opath, &start, &length)) {
        return NULL;
    }

    path = Utils::getObjectAsUTF8String(opath);
    if (!path) {
        PyErr_SetString(PyExc_ValueError, "Unable to parse the path.");
        return NULL;
    }

    char*** hosts = hdfsGetHosts(self->_fs, path, start, length);
    if (!hosts) {
        PyErr_SetString(PyExc_RuntimeError, "Failed to get block information");
        return NULL;
    }

    int numberOfBlocks = 0;
    PyObject* result = PyList_New(0);

    while(hosts[numberOfBlocks]) {

        PyObject* hostsBlocks = PyList_New(0);

        int numberOfBlockHosts = 0;
        while(hosts[numberOfBlocks][numberOfBlockHosts])   {

            PyList_Append(hostsBlocks, PyString_FromString(hosts[numberOfBlocks][numberOfBlockHosts]));
            numberOfBlockHosts++;
        }

        PyList_Append(result, hostsBlocks);
        numberOfBlocks++;
    }

    hdfsFreeHosts(hosts);

    return result;
}

PyObject* FsClass_default_block_size(FsInfo* self) {
    return FsClass_get_default_block_size(self);
}


PyObject* FsClass_get_default_block_size(FsInfo* self) {
    tOffset size = hdfsGetDefaultBlockSize(self->_fs);
    return PyLong_FromSsize_t(size);
}

PyObject* FsClass_used(FsInfo* self) {
    return FsClass_get_used(self);
}

PyObject* FsClass_get_used(FsInfo* self) {

    tOffset size = hdfsGetUsed(self->_fs);
    return PyLong_FromSsize_t(size);
}

PyObject* FsClass_set_replication(FsInfo* self, PyObject* args, PyObject* kwds) {

    const char *path;
    short replication;
    PyObject *opath;


    if (!PyArg_ParseTuple(args, "Oh", &opath, &replication)) {
        return NULL;
    }

    path = Utils::getObjectAsUTF8String(opath);
    if (!path) {
        PyErr_SetString(PyExc_ValueError, "Unable to parse the path.");
        return NULL;
    }

    int result = hdfsSetReplication(self->_fs, path, replication);
    return PyBool_FromLong(result >= 0 ? 1 : 0);
}


PyObject* FsClass_set_working_directory(FsInfo* self, PyObject* args, PyObject* kwds) {

    const char* path;
    PyObject *opath;

    if (!PyArg_ParseTuple(args, "O", &opath)) {
        return NULL;
    }

    path = Utils::getObjectAsUTF8String(opath);
    if (!path) {
        PyErr_SetString(PyExc_ValueError, "Unable to parse the path.");
        return NULL;
    }

    int result = hdfsSetWorkingDirectory(self->_fs, path);
    return PyBool_FromLong(result >= 0 ? 1 : 0);
}


PyObject* FsClass_open_file(FsInfo* self, PyObject *args, PyObject *kwds)
{
    char* path;
    PyObject *opath;
    int flags, buff_size, blocksize, readline_chunk_size;
    short replication;


    if (!PyArg_ParseTuple(args, "O|iihii",
            &opath, &flags, &buff_size, &replication, &blocksize, &readline_chunk_size)) {
        return NULL;
    }

    path = Utils::getObjectAsUTF8String(opath);
    if (path == NULL) {
        PyErr_SetString(PyExc_IOError, "Unable to parse the argument.");
        return NULL;
    }

    hdfsFile file = hdfsOpenFile(self->_fs, path, flags, buff_size, replication, blocksize);
    if (file == NULL) {
        return PyErr_SetFromErrno(PyExc_IOError);
    }

    PyObject* module = PyImport_ImportModule("native_core_hdfs");

    PyObject* obj_instance = PyObject_CallMethod(module, "CoreHdfsFile","OO", self->_fs, file); //, flags, buff_size, replication, blocksize, NULL);

    FileInfo *fileInfo = ((FileInfo*) obj_instance);
    fileInfo->path = path;
    fileInfo->flags = flags;
    fileInfo->buff_size = buff_size;
    fileInfo->blocksize = blocksize;
    fileInfo->replication = replication;
    fileInfo->readline_chunk_size = readline_chunk_size;

    #ifdef HADOOP_LIBHDFS_V1
        fileInfo->stream_type = (((flags & O_WRONLY) == 0) ? INPUT : OUTPUT);
    #endif

    return obj_instance;
}


PyObject *FsClass_name(FsInfo* self)
{
    PyObject* result = PyString_FromFormat("%s %d %s", self->host, self->port, self->user);
    if (!result)
        PyErr_SetString(PyExc_RuntimeError, "Failed to format class name");

    return result;
}


PyObject *FsClass_capacity(FsInfo *self) {
    return FsClass_get_capacity(self);
}


PyObject *FsClass_get_capacity(FsInfo *self) {
    errno = 0; // hdfsGetCapacity forgets to clear errno
    tOffset capacity = hdfsGetCapacity(self->_fs);
    if (capacity < 0) {
        // two error cases are contemplated by the code in hdfsGetCapacity:
        // 1) exception from the Java method
        // 2) FS instance is not a DistributedFileSystem.
        // Here we copy their error textually.
        if (errno)
            PyErr_SetFromErrno(PyExc_IOError);
        else {
            PyErr_SetString(PyExc_RuntimeError,
                    "hdfsGetCapacity works only on a DistributedFileSystem");
        }

        return NULL;
    }
    return PyLong_FromSsize_t(capacity);
}


PyObject* FsClass_copy(FsInfo* self, PyObject *args, PyObject *kwds)
{
    FsInfo* to_hdfs;
    const char *from_path, *to_path;
    PyObject *o_from_path, *o_to_path;

    if (! PyArg_ParseTuple(args, "OOO", &o_from_path, &to_hdfs, &o_to_path)) {
        return NULL;
    }

    from_path = Utils::getObjectAsUTF8String(o_from_path);
    if (!from_path) {
        PyErr_SetString(PyExc_ValueError, "Unable to parse source path.");
        return NULL;
    }

    to_path = Utils::getObjectAsUTF8String(o_to_path);
    if (!o_to_path) {
        PyErr_SetString(PyExc_ValueError, "Unable to parse destination path.");
        return NULL;
    }

    int result = hdfsCopy(self->_fs, from_path, to_hdfs->_fs, to_path);
    if (result < 0)
        return PyErr_SetFromErrno(PyExc_RuntimeError);

    return PyLong_FromLong(result);
}


PyObject *FsClass_exists(FsInfo *self, PyObject *args, PyObject *kwds) {

    const char *path;
    PyObject *opath;

    if (! PyArg_ParseTuple(args, "O", &opath)) {
        return NULL;
    }

    path = Utils::getObjectAsUTF8String(opath);
    if (!path) {
        PyErr_SetString(PyExc_ValueError, "Unable to parse the path.");
        return NULL;
    }

    int result = hdfsExists(self->_fs, path);

    // LP: hdfsExists (in some cases?) sets errno to ENOENT "[Errno 2] No such
    // file or directory" when the path doesn't exist or EEXIST in other cases.
    // I don't know why.  Since that's what we're trying to test, I'll skip
    // checking errno here.  The consequence is that when we return false it
    // may be because of an error and not because the path doesn't exist.
    //
    // if (result < 0 && errno) return PyErr_SetFromErrno(PyExc_IOError);

    return PyBool_FromLong(result >= 0 ? 1 : 0);
}


PyObject *FsClass_create_directory(FsInfo *self, PyObject *args, PyObject *kwds) {

    const char *path;
    PyObject *opath;

    if (! PyArg_ParseTuple(args, "O", &opath)) {
        return NULL;
    }

    path = Utils::getObjectAsUTF8String(opath);
    if (!path) {
        PyErr_SetString(PyExc_ValueError, "Unable to parse the path.");
        return NULL;
    }

    int result = hdfsCreateDirectory(self->_fs, path);
    if (result < 0)
        return PyErr_SetFromErrno(PyExc_IOError);

    return PyBool_FromLong(result >= 0 ? 1 : 0);
}

/*
 * Works on borrowed reference `dict`.
 *
 * \return 0 if successful
 * \return -1 if there was a problem. In that case, dict may contain
 * some values, but will be incomplete and should be discarded.
 */
int setPathInfo(PyObject* dict, hdfsFileInfo* fileInfo) {

    if (dict == NULL || fileInfo == NULL) return -1;
    int error_code = 0;

    const char*const keys[] = {
        "name",
        "kind",
        "group",
        "last_mod",
        "last_access",
        "replication",
        "owner",
        "permissions",
        "block_size",
        "path",
        "size"
    };

    const int n_fields = sizeof(keys) / sizeof(keys[0]);

    PyObject* values[n_fields];
    int i = 0;
    // Prepare the values.  We'll check for all errors in the "set" loop below
    // The order of these values MUST match the order of the keys above
    values[i++] = PyUnicode_FromString(fileInfo->mName);
    values[i++] = PyString_FromString(fileInfo->mKind == kObjectKindDirectory ? "directory" : "file");
    values[i++] = PyString_FromString(fileInfo->mGroup);
    values[i++] = PyInt_FromLong(fileInfo->mLastMod);
    values[i++] = PyInt_FromLong(fileInfo->mLastAccess);
    values[i++] = PyInt_FromSize_t(fileInfo->mReplication);
    values[i++] = PyString_FromString(fileInfo->mOwner);
    values[i++] = PyInt_FromSize_t(fileInfo->mPermissions);
    values[i++] = PyInt_FromLong(fileInfo->mBlockSize);
    values[i++] = PyUnicode_FromString(fileInfo->mName);
    values[i++] = PyLong_FromLongLong(fileInfo->mSize);

    for (i = 0; i < n_fields; ++i) {
        if (values[i] == NULL || PyDict_SetItemString(dict, keys[i], values[i]) < 0) {
            error_code = -1;
            break;
            // Don't DECREF here.  The error handling code goes through the entire array
            // and thus we'd end up DECREFing some objects twice.
        }
    }

    for (i = 0; i < n_fields; ++i) {
        Py_XDECREF(values[i]); // some values may be null (if there was an error
    }

    return error_code;
}

PyObject *FsClass_list_directory(FsInfo *self, PyObject *args, PyObject *kwds) {
    const char *path;
    PyObject *opath;

    if (!PyArg_ParseTuple(args, "O",  &opath)) {
        return NULL;
    }

    path = Utils::getObjectAsUTF8String(opath);
    if (!path) {
        PyErr_SetString(PyExc_ValueError, "Unable to parse the path.");
        return NULL;
    }

    errno = 0;
    hdfsFileInfo* pathInfo = hdfsGetPathInfo(self->_fs, path);
    if (!pathInfo) {
        return PyErr_SetFromErrno(PyExc_IOError);
    }

    PyObject *result = NULL;
    hdfsFileInfo* pathList = NULL;
    int numEntries = 0;

    if (pathInfo->mKind == kObjectKindDirectory) {

        errno = 0;
        pathList = hdfsListDirectory(self->_fs, pathInfo->mName, &numEntries);
        // hdfsListDirectory returns NULL when a directory is empty, so to determine
        // whether there's been an error we also need to check errno
        if (!pathList && errno) {
            PyErr_SetFromErrno(PyExc_IOError);
            goto error;
        }
    }
    else {
        numEntries = 1;
        pathList = pathInfo;
        pathInfo = NULL;
    }

    result = PyList_New(numEntries);
    if (!result) goto mem_error;

    for (Py_ssize_t i = 0; i < numEntries; i++) {
        PyObject* infoDict = PyDict_New();
        if (!infoDict) goto mem_error;
        PyList_SET_ITEM(result, i, infoDict);
        if (setPathInfo(infoDict, &pathList[i]) < 0) {
            PyErr_SetString(PyExc_IOError, "Error getting file info");
            goto error;
        }
    }

    goto clean_up; // skip the error section

mem_error:
    PyErr_SetString(PyExc_MemoryError, "Error allocating structures");
    // fall through
error:
    // in case of error DECREF our result structure and return NULL
    if (result != NULL) {
        Py_XDECREF(result);
        result = NULL;
    }

clean_up:
    // all code paths go through the clean_up section
    if (pathInfo != NULL)
        hdfsFreeFileInfo(pathInfo, 1);
    if (pathList != NULL)
        hdfsFreeFileInfo(pathList, numEntries);

    return result;
}

PyObject *FsClass_move(FsInfo *self, PyObject *args, PyObject *kwds) {

    FsInfo* to_hdfs;
    const char *from_path, *to_path;
    PyObject *o_from_path, *o_to_path;


    if (! PyArg_ParseTuple(args, "OOO", &o_from_path, &to_hdfs, &o_to_path)) {
        return NULL;
    }

    from_path = Utils::getObjectAsUTF8String(o_from_path);
    if (!from_path) {
        PyErr_SetString(PyExc_ValueError, "Unable to parse the source path.");
        return NULL;
    }

    to_path = Utils::getObjectAsUTF8String(o_to_path);
    if (!o_to_path) {
        PyErr_SetString(PyExc_ValueError, "Unable to parse the destination path.");
        return NULL;
    }

    int result = hdfsMove(self->_fs, from_path, to_hdfs->_fs, to_path);
    if (result < 0)
        return PyErr_SetFromErrno(PyExc_IOError);

    return PyBool_FromLong(result >= 0 ? 1 : 0);
}


PyObject *FsClass_rename(FsInfo *self, PyObject *args, PyObject *kwds) {

    const char *from_path, *to_path;
    PyObject *o_from_path, *o_to_path;

    if (! PyArg_ParseTuple(args, "OO", &o_from_path, &o_to_path)) {
        return NULL;
    }

    from_path = Utils::getObjectAsUTF8String(o_from_path);
    if (!from_path) {
        PyErr_SetString(PyExc_ValueError, "Unable to parse the source path.");
        return NULL;
    }

    to_path = Utils::getObjectAsUTF8String(o_to_path);
    if (!o_to_path) {
        PyErr_SetString(PyExc_ValueError, "Unable to parse the destination path.");
        return NULL;
    }

    int result = hdfsRename(self->_fs, from_path, to_path);
    if (result < 0)
        return PyErr_SetFromErrno(PyExc_IOError);

    return PyBool_FromLong(result >= 0 ? 1 : 0);
}


PyObject *FsClass_delete(FsInfo *self, PyObject *args, PyObject *kwds) {

    const char *path;
    PyObject *opath;
    int recursive = 1;

    if (!PyArg_ParseTuple(args, "O|i", &opath, &recursive)) {
        return NULL;
    }

    path = Utils::getObjectAsUTF8String(opath);
    if (!path) {
        PyErr_SetString(PyExc_ValueError, "Unable to parse the path.");
        return NULL;
    }

    #ifdef HADOOP_LIBHDFS_V1
    int result = hdfsDelete(self->_fs, path);
    #else
    int result = hdfsDelete(self->_fs, path, recursive);
    #endif

    if (result < 0)
        return PyErr_SetFromErrno(PyExc_IOError);

    return PyBool_FromLong(result >= 0 ? 1 : 0);
}


PyObject *FsClass_chmod(FsInfo *self, PyObject *args, PyObject *kwds) {

    const char *path;
    PyObject *opath;
    short mode = 1;

    if (! PyArg_ParseTuple(args, "Oh", &opath, &mode)) {
        return NULL;
    }

    path = Utils::getObjectAsUTF8String(opath);
    if (!path) {
        PyErr_SetString(PyExc_ValueError, "Unable to parse the path.");
        return NULL;
    }

    // hdfsChmod doesn't always set errno in case of error.  We clear it
    // here so that after the call we'll be sure we're not looking at an old value
    errno = 0;
    int result = hdfsChmod(self->_fs, path, mode);
    if (result < 0) {
        // hdfsChmod
        if (errno)
            return PyErr_SetFromErrno(PyExc_IOError);
        else {
            PyErr_SetString(PyExc_IOError, "Unknown error while changing permissions");
            return NULL;
        }
    }
    // else

    return PyBool_FromLong(result >= 0 ? 1 : 0);
}


PyObject *FsClass_chown(FsInfo *self, PyObject *args, PyObject *kwds) {

    const char *path, *user, *group;
    PyObject *opath;

    if (! PyArg_ParseTuple(args, "O|ss", &opath, &user, &group)) {
        return 0;
    }

    path = Utils::getObjectAsUTF8String(opath);
    if (!path) {
        PyErr_SetString(PyExc_ValueError, "Unable to parse the path.");
        return NULL;
    }

    hdfsFileInfo* fileInfo = hdfsGetPathInfo(self->_fs, path);
    if (!fileInfo) {
        return PyErr_SetFromErrno(PyExc_IOError);
    }

    if (str_empty(user))
         user = fileInfo->mOwner;

    if (str_empty(group))
        group = fileInfo->mGroup;

    int result = hdfsChown(self->_fs, path, user, group);
    hdfsFreeFileInfo(fileInfo, 1);

    if (result < 0)
        return PyErr_SetFromErrno(PyExc_IOError);

    return PyBool_FromLong(result >= 0 ? 1 : 0);
}


PyObject *FsClass_utime(FsInfo *self, PyObject *args, PyObject *kwds) {

    const char *path;
    PyObject *opath;
    tTime mtime, atime;

    if (! PyArg_ParseTuple(args, "Oll", &opath, &mtime, &atime)) {
        return NULL;
    }

    path = Utils::getObjectAsUTF8String(opath);
    if (!path) {
        PyErr_SetString(PyExc_ValueError, "Unable to parse the path.");
        return NULL;
    }

    int result = hdfsUtime(self->_fs, path, mtime, atime);
    if (result < 0)
        return PyErr_SetFromErrno(PyExc_IOError);

    return PyBool_FromLong(result >= 0 ? 1 : 0);
}

