#include "hdfs_fs.hpp"

#define exec_and_trap_error(res_type_t, what,err_msg)	\
  res_type_t res = what;\
  if (res < 0){\
    throw hdfs_exception(err_msg);\
  }

//--
void wrap_hdfs_fs::disconnect(){
  exec_and_trap_error(int, hdfsDisconnect(fs_), "Cannot disconnect from " + host_);
}
//--
tOffset wrap_hdfs_fs::get_default_block_size(){
  exec_and_trap_error(tOffset, hdfsGetDefaultBlockSize(fs_), 
		      "Cannot get default block_size of filesystem on" + host_);
  return res;
}
//--
tOffset wrap_hdfs_fs::get_capacity(){
  exec_and_trap_error(tOffset, hdfsGetCapacity(fs_),
		      "Cannot get capacity of filesystem on" + host_);
  return res;
}

tOffset wrap_hdfs_fs::get_used(){
  exec_and_trap_error(tOffset, hdfsGetUsed(fs_),
		      "Cannot get amount of space used of filesystem on" + host_);
  return res;
}

bool wrap_hdfs_fs::exists(const std::string& path){
  int res = hdfsExists(fs_, path.c_str());
  return (res == 0)? true : false;
}

void wrap_hdfs_fs::unlink(const std::string& path){
  exec_and_trap_error(int, hdfsDelete(fs_, path.c_str()), 
		      "Cannot delete " + path 
		      + " in filesystem on " + host_);
}
//--
void wrap_hdfs_fs::copy(const std::string& path,
			wrap_hdfs_fs& dst_fs, const std::string& dst_path) {
  std::string err_msg = "Cannot copy " + path + " to filesystem on " + dst_fs.host_;
  exec_and_trap_error(int, 
		      hdfsCopy(fs_, path.c_str(),
			       dst_fs.fs_, dst_path.c_str()),
		      err_msg);
}
//--
void wrap_hdfs_fs::move(const std::string& path,
			wrap_hdfs_fs& dst_fs, const std::string& dst_path) {
  std::string err_msg = "Cannot move " + path + " to filesystem on " + dst_fs.host_;
  exec_and_trap_error(int, 
		      hdfsMove(fs_, path.c_str(),
			       dst_fs.fs_, dst_path.c_str()),
		      err_msg);
}
//--
void wrap_hdfs_fs::rename(const std::string& old_path,
			  const std::string& new_path) {
  std::string err_msg = "Cannot rename " + old_path + " to " + new_path;
  exec_and_trap_error(int, 
		      hdfsRename(fs_, old_path.c_str(), new_path.c_str()),
		      err_msg);
}
//--
std::string wrap_hdfs_fs::get_working_directory() {
  std::size_t buff_size = 1024;
  char* buff = new char[buff_size];
  exec_and_trap_error(char*, 
		      hdfsGetWorkingDirectory(fs_, buff, buff_size),
		      "Cannot get working directory");
  std::string cwd(buff);
  return cwd;
}
//--
void wrap_hdfs_fs::set_working_directory(const std::string& path){
  exec_and_trap_error(int, 
		      hdfsSetWorkingDirectory(fs_, path.c_str()),
		      "Cannot set working directory to " + path);
}

//--
void wrap_hdfs_fs::create_directory(const std::string& path) {
  exec_and_trap_error(int, 
		      hdfsCreateDirectory(fs_, path.c_str()),
		      "Cannot create directory " + path);
}
//--
void wrap_hdfs_fs::set_replication(const std::string& path, int replication){
  exec_and_trap_error(int, 
		      hdfsSetReplication(fs_, path.c_str(), replication),
		      "Cannot set replication of " + path);
}

//--
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
    .def("copy",    &wrap_hdfs_fs::copy)
    .def("move",    &wrap_hdfs_fs::move)
    .def("rename",    &wrap_hdfs_fs::rename)
    .def("working_directory",     &wrap_hdfs_fs::get_working_directory)
    .def("set_working_directory", &wrap_hdfs_fs::set_working_directory)
    .def("create_directory", &wrap_hdfs_fs::create_directory)
    .def("set_replication", &wrap_hdfs_fs::set_replication)
    .def("open_file", &wrap_hdfs_fs::open_file,
	 return_value_policy<manage_new_object>())
    ;
}



