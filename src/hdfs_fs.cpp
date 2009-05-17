#include "hdfs_fs.hpp"

void wrap_hdfs_fs::disconnect(){
  int res = hdfsDisconnect(fs_);
  if (res < 0){
    throw hdfs_exception("Cannot disconnect from " + host_);    
  }
}

tOffset wrap_hdfs_fs::get_default_block_size(){
  tOffset res = hdfsGetDefaultBlockSize(fs_);
  if (res < 0){
    throw hdfs_exception("Cannot get default block_size of filesystem on" + host_);        
  }
}
tOffset wrap_hdfs_fs::get_capacity(){
  tOffset res = hdfsGetCapacity(fs_);
  if (res < 0){
    throw hdfs_exception("Cannot get capacity of filesystem on" + host_);        
  }
}
tOffset wrap_hdfs_fs::get_used(){
  tOffset res = hdfsGetUsed(fs_);
  if (res < 0){
    throw hdfs_exception("Cannot get amount of space used of filesystem on" + host_);        
  }
}

bool wrap_hdfs_fs::exists(const std::string& path){
  int res = hdfsExists(fs_, path.c_str());
  if (res < 0){
    throw hdfs_exception("Cannot check existence of " 
			 + path + " in filesystem on " 
			 + host_);        
  }
  return (res == 0)? true : false;
}

void wrap_hdfs_fs::unlink(const std::string& path){
  int res = hdfsDelete(fs_, path.c_str());
  if (res < 0){
    throw hdfs_exception("Cannot delete " 
			 + path + " in filesystem on " 
			 + host_);        
  }
}


wrap_hdfs_file* wrap_hdfs_fs::open_file(std::string path, int flags, 
					int buffer_size, int replication, 
					int blocksize) {
  const char* c_path = (path.size() > 0) ? path.c_str() : NULL; 
  std::cerr << "size of path =" << path.size() << std::endl;
  hdfsFile f = hdfsOpenFile(fs_, c_path, flags, buffer_size,
			    replication, blocksize);
  if (f == NULL){
    throw hdfs_exception("Cannot open file " 
			 + path + " in filesystem on " 
			 + host_);        
  }
  return new wrap_hdfs_file(path, this, f);
}

//+++++++++++++++++++++++++++++++++++++++++
// Exporting class definitions.
//+++++++++++++++++++++++++++++++++++++++++
void export_hdfs_fs() 
{
  using namespace boost::python;
  //--
  class_<wrap_hdfs_fs, boost::noncopyable>("hdfs_fs", init<std::string, int>())
    .def("close", &wrap_hdfs_fs::disconnect)
    .def("capacity",  &wrap_hdfs_fs::get_capacity)
    .def("default_block_size",  &wrap_hdfs_fs::get_default_block_size)
    .def("used",  &wrap_hdfs_fs::get_used)
    .def("exists",  &wrap_hdfs_fs::exists)
    .def("delete",  &wrap_hdfs_fs::unlink)
    .def("open_file", &wrap_hdfs_fs::open_file,
	 return_value_policy<manage_new_object>())
    ;
}



