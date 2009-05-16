#include "hdfs_fs.hpp"


wrap_hdfs_file* wrap_hdfs_fs::open_file(std::string path, int flags, 
					int buffer_size, int replication, 
					int blocksize) {
  const char* c_path = (path.size() > 0) ? path.c_str() : NULL; 
  std::cerr << "size of path =" << path.size() << std::endl;
  hdfsFile f = hdfsOpenFile(fs_, c_path, flags, buffer_size,
			    replication, blocksize);
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
    .def("open_file", &wrap_hdfs_fs::open_file,
	 return_value_policy<manage_new_object>())
    ;
}



