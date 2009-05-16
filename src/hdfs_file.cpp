#include "hdfs_fs.hpp"
#include "hdfs_file.hpp"

    
//-----------------------------------------------
void wrap_hdfs_file::seek(tOffset desidered_pos){
  int ret = hdfsSeek(fs_->fs_, file_, desidered_pos);
  if (ret < 0){
    throw hdfs_exception("Cannot seek on " + filename_);
  }
}
//-----------------------------------------------
int wrap_hdfs_file::tell() {
  int c_offset = hdfsTell(fs_->fs_, file_);
  if (c_offset < 0) {
    throw hdfs_exception("Cannot tell on " + filename_);
  }
  return c_offset;
}
//-----------------------------------------------
std::string wrap_hdfs_file::read(tSize length) {
  char* buf = new char[length];
  tSize bytes_read = hdfsRead(fs_->fs_, file_, 
			      static_cast<void*>(buf), length);
  if (bytes_read < 0){
    throw hdfs_exception("Cannot read on " + filename_);
  }
  std::string res(buf, bytes_read);
  return res;
}
//-----------------------------------------------
std::string wrap_hdfs_file::pread(tOffset position, tSize length) {
  char* buf = new char[length];
  tSize bytes_read = hdfsRead(fs_->fs_, file_, 
			      static_cast<void*>(buf), length);
  if (bytes_read < 0){
    throw hdfs_exception("Cannot pread on " + filename_);
  }
  std::string res(buf, bytes_read);
  return res;
}
//-----------------------------------------------
tSize wrap_hdfs_file::write(std::string buffer){
  tSize bytes_written = hdfsWrite(fs_->fs_, file_, 
				  static_cast<const void*>(buffer.c_str()),
				  buffer.size());
  if (bytes_written < 0){
    throw hdfs_exception("Cannot write on " + filename_);
  }
  return bytes_written;
}
//-----------------------------------------------
#if 0
//-----------------------------------------------
int read_chunk() {
}
//-----------------------------------------------
int pread_chunk() {}
#endif

//-----------------------------------------------
void wrap_hdfs_file::_close_helper() {
  if (is_open_){
    int res = hdfsCloseFile(fs_->fs_, file_);
    if (res == -1){
      throw hdfs_exception("Cannot close " + filename_);
    }
    is_open_ = false;
  }
}

//+++++++++++++++++++++++++++++++++++++++++
// Exporting class definitions.
//+++++++++++++++++++++++++++++++++++++++++
void export_hdfs_file() 
{
  using namespace boost::python;
  //--
  class_<wrap_hdfs_file, boost::noncopyable>("hdfs_file", no_init)
    .def("seek", &wrap_hdfs_file::seek)
    .def("tell", &wrap_hdfs_file::tell)
    .def("read", &wrap_hdfs_file::read)
    .def("pread", &wrap_hdfs_file::pread)
    .def("write", &wrap_hdfs_file::write)
    ;
}

