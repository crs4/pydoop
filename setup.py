# BEGIN_COPYRIGHT
# END_COPYRIGHT

# Important environment variables
# ---------------------------------
#
# The Pydoop setup looks in a number of default paths for what it needs.  If necessary, you
# can override its behaviour or provide an alternative path by exporting the environment
# variables below.
#
# HADOOP_HOME:  tell setup where your hadoop home is
# HADOOP_SRC:  tell setup where to find the Hadoop source, if it's not under HADOOP_HOME/src or /usr/src/hadoop-*
# HADOOP_INCLUDE_PATHS:  override the standard Hadoop include paths:
#     src/c++/{pipes,utils}/api/hadoop, 
#     /usr/include, 
#     src/mapred/c++/Linux-{arch}/include/hadoop
#     src/c++/Linux-{arch}/include/hadoop
# JAVA_HOME:  by default looks  in /opt/sun-jdk and /usr/lib/jvm/java-6-sun
# HADOOP_VERSION:  override the version returned by running "hadoop version" (and avoid running the hadoop binary)

import sys, os, platform, re
from distutils.core import setup
from distutils.extension import Extension
from distutils.command.build_ext import build_ext

import pydoop
from pydoop.hadoop_utils import get_hadoop_version

PipesSrc = ["pipes", "pipes_context", "pipes_test_support",
          "pipes_serial_utils", "exceptions", "pipes_input_split"]
HdfsSrc = ["hdfs_fs", "hdfs_file", "hdfs_common"]
PipesExtName = "_pipes"
HdfsExtName = "_hdfs"

# this should be more reliable than deciding based on hadoop version
def get_hdfs_macros(hdfs_hdr):
  hdfs_macros = []
  f = open(hdfs_hdr)
  t = f.read()
  f.close()
  delete_args = re.search(r"hdfsDelete\((.+)\)", t).groups()[0].split(",")
  cas_args = re.search(r"hdfsConnectAsUser\((.+)\)", t).groups()[0].split(",")
  if len(delete_args) > 2:
    hdfs_macros.append(("RECURSIVE_DELETE", None))
  if len(cas_args) > 3:
    hdfs_macros.append(("CONNECT_GROUP_INFO", None))
  return hdfs_macros

def get_arch():
  bits, linkage = platform.architecture()
  if bits == "64bit":
    return "amd64", "64"
  return "i386", "32"


def get_java_include_dirs(java_home):
  p = platform.system().lower()  # TODO: test for non-linux
  java_inc = os.path.join(java_home, "include")
  java_platform_inc = "%s/%s" % (java_inc, p)
  return [java_inc, java_platform_inc]

def get_java_library_dirs(java_home):
  a = get_arch()[0]
  return [os.path.join(java_home, "jre/lib/%s/server" % a)]


class BoostExtension(Extension):

  export_pattern = re.compile(r"void\s+export_(\w+)")

  def __init__(self, name, wrap_sources, aux_sources, patches=None, **kw):
    Extension.__init__(self, name, wrap_sources+aux_sources, **kw)
    self.module_name = self.name.rsplit(".", 1)[-1]
    self.wrap_sources = wrap_sources
    self.patches = patches

  def __must_generate(self, target, prerequisites):
    try:
      return max(mtime(p) for p in prerequisites) > mtime(target)
    except OSError:
      return True

  def generate_main(self):
    destdir = os.path.split(self.wrap_sources[0])[0]  # should be ok
    outfn = os.path.join(destdir, "%s_main.cpp" % self.module_name)
    if self.__must_generate(outfn, self.wrap_sources):
      sys.stderr.write("generating main for %s\n" % self.name)
      first_half = ["#include <boost/python.hpp>"]
      second_half = ["BOOST_PYTHON_MODULE(%s){" % self.module_name]
      for fn in self.wrap_sources:
        f = open(fn)
        code = f.read()
        f.close()
        m = self.export_pattern.search(code)
        if m is not None:
          fun_name = "export_%s" % m.groups()[0]
          first_half.append("void %s();" % fun_name)
          second_half.append("%s();" % fun_name)
      second_half.append("}")
      outf = open(outfn, "w")
      for line in first_half:
        outf.write("%s%s" % (line, os.linesep))
      for line in second_half:
        outf.write("%s%s" % (line, os.linesep))
      outf.close()
    return outfn

  def generate_patched_aux(self):
    aux = []
    if not self.patches:
      return aux
    for fn, p in self.patches.iteritems():
      patched_fn = "src/%s" % os.path.basename(fn)
      # FIXME: the patch should also be listed as a prerequisite.
      if self.__must_generate(patched_fn, [fn]):
        sys.stderr.write("copying and patching %s\n" % fn)
        f = open(fn)
        contents = f.read()
        f.close()
        for old, new in self.patches[fn].iteritems():
          contents = contents.replace(old, new)
        f = open(patched_fn, "w")
        f.write(contents)
        f.close()
      aux.append(patched_fn)
    return aux

def create_basic_pipes_ext():
  return BoostExtension(PipesExtName, ["src/%s.cpp" % n for n in PipesSrc], [])

def create_basic_hdfs_ext():
  return BoostExtension(HdfsExtName, ["src/%s.cpp" % n for n in HdfsSrc], [])

def create_full_pipes_ext(path_finder):
  basedir = path_finder.mapred_src
  patches = {
    os.path.join(basedir, "utils/impl/SerialUtils.cc"): {
      OLD_DESERIALIZE_FLOAT: NEW_DESERIALIZE_FLOAT
      },
    os.path.join(basedir, "utils/impl/StringUtils.cc"): {
      },
    os.path.join(basedir, "pipes/impl/HadoopPipes.cc"): {
      OLD_WRITE_BUFFER: NEW_WRITE_BUFFER
      },
    }
  include_dirs = path_finder.mapred_inc
  libraries = ["pthread", "boost_python"]
  if path_finder.hadoop_version[2] == 203:
    include_dirs.append("/usr/include/openssl")
    libraries.append("ssl")
  return BoostExtension(
    pydoop.complete_mod_name(PipesExtName, path_finder.hadoop_version),
    ["src/%s.cpp" % n for n in PipesSrc],
    [], # aux
    patches=patches,
    include_dirs=include_dirs,
    libraries=libraries
    )

def create_full_hdfs_ext(path_finder):
  library_dirs = get_java_library_dirs(path_finder.java_home) + path_finder.hdfs_link_paths["L"]
  return BoostExtension(
    pydoop.complete_mod_name(HdfsExtName, path_finder.hadoop_version),
    ["src/%s.cpp" % n for n in HdfsSrc],
    [], # aux
    include_dirs=get_java_include_dirs(path_finder.java_home) + [path_finder.hdfs_inc_path],
    library_dirs=library_dirs,
    runtime_library_dirs=library_dirs,
    libraries=["pthread", "boost_python", "hdfs", "jvm"],
    define_macros=get_hdfs_macros(os.path.join(path_finder.hdfs_inc_path, "hdfs.h"))
    )


class build_pydoop_ext(build_ext):
  def finalize_options(self):
    build_ext.finalize_options(self)

    path_finder = PathFinder()

    self.extensions = [ create_full_pipes_ext(path_finder), create_full_hdfs_ext(path_finder) ]

    for e in self.extensions:
      e.sources.append(e.generate_main())
      e.sources.extend(e.generate_patched_aux())


def create_ext_modules():
  ext_modules = []
  ext_modules.append(create_basic_pipes_ext())
  ext_modules.append(create_basic_hdfs_ext())
  return ext_modules

def find_first_existing(*paths):
  for p in paths:
    if os.path.exists(p):
      return p
  return None

class PathFinder(object):
  def __init__(self):
    self.java_home = None
    self.hadoop_home = None
    self.hadoop_version = None
    self.src = None
    self.mapred_src = None
    self.mapred_inc = []
    self.hdfs_inc_path = None # special case, only one include path since we only have one file
    self.hdfs_link_paths = { "L":[], "l":[] }
    self.__init_paths()

  def cloudera(self):
    return len(self.hadoop_version) > 3 and re.match("cdh.*", self.hadoop_version[3] or "")

  # returns one of:
  #   1. HADOOP_SRC
  #   2. HADOOP_HOME/src
  #   3. /usr/src/hadoop*
  #   4. None
  # Returns the path if found
  def __find_hadoop_src(self):
    if os.getenv("HADOOP_SRC"):
      return os.getenv("HADOOP_SRC")
    if self.hadoop_home:
      if os.path.exists( os.path.join(self.hadoop_home, "src") ):
        return os.path.join(self.hadoop_home, "src")

    # look in /usr/src
    usr_src = os.path.join( os.path.sep, "usr", "src" )
    if os.path.exists(usr_src ):
      path_list = [ path for path in os.listdir(usr_src) if re.match(r"hadoop\b.*", path) ]
      if len(path_list) > 1:
        path_list = sorted(path_list)
      if path_list: # if non-empty
        return os.path.join(usr_src, path_list[0])

    return None # haven't found anything

  def __set_mapred_inc_paths(self):
    if os.getenv("HADOOP_INCLUDE_PATHS"):
      self.mapred_inc = os.getenv("HADOOP_INCLUDE_PATHS").split(os.pathsep)
      return

    # look in the source first
    src_paths = [ os.path.join( self.src, "c++", "pipes", "api", "hadoop" ), os.path.join( self.src, "c++", "utils", "api", "hadoop") ]
    if all( map(os.path.exists, src_paths) ):
      self.mapred_inc = map(os.path.dirname, src_paths) # the includes are for "hadoop/<file.h>", so we chop the hadoop directory off the path
    else:
      if self.cloudera():
        # we didn't find the expected include paths in the source.  Try the standard /usr/include/hadoop
        usr_inc_hadoop = os.path.join( os.path.sep, "usr", "include", "hadoop")
        if os.path.exists( usr_inc_hadoop ):
          self.mapred_inc = [ os.path.dirname(usr_inc_hadoop) ]
        else:
          msg = "Couldn't find Hadoop c++ include directory.  Searched in: \n  " + "\n  ".join(src_paths + [ usr_inc_hadoop ]) + "\nTry specifying one with HADOOP_INC_PATH"
          raise RuntimeError("Couldn't find Hadoop c++ include directories in source or in /usr/include/hadoop")
      else: # Apache hadoop
        arch_string = "-".join(get_arch())
        search_paths = (
          os.path.join(self.src, "mapred", "c++", "Linux-%s" % arch_string, "include", "hadoop"),
          os.path.join(self.src, "c++", "Linux-%s" % arch_string, "include", "hadoop"),
          os.path.join(os.path.sep, "usr", "include", "hadoop"))
        inc_path = find_first_existing( *search_paths )

        if inc_path:
          self.mapred_inc = [ os.path.dirname(inc_path) ]
        else:
          msg = "Couldn't find Hadoop c++ include directory.  Searched in: \n  " + "\n  ".join(search_paths + src_paths) + "\nTry specifying one with HADOOP_INC_PATH"
          raise RuntimeError(msg)

  def __set_hdfs_link_paths(self):
    self.hdfs_link_paths["l"].append("hdfs") # link to libhdfs

    # But, where to find libhdfs?
    lib = find_first_existing(
      os.path.join(os.path.sep, "usr", "lib", "libhdfs.so"),
      os.path.join(self.hadoop_home, "hdfs", "c++", "Linux-%s-%s" % get_arch(), "lib", "libhdfs.so"),
      os.path.join(self.hadoop_home, "c++", "Linux-%s-%s" % get_arch(), "lib", "libhdfs.so"),
      )
    if lib:
      dir, name = os.path.split(lib)
      if dir != os.path.join(os.path.sep, "usr", "lib"):
        self.hdfs_link_paths["L"].append(dir)
    else:
      raise RuntimeError("Couldn't find libhdfs.so in HADOOP_HOME or /usr/lib.")
        
  def __set_hdfs_inc_path(self):
    fname = find_first_existing(
      os.path.join(self.src, "c++", "libhdfs", "hdfs.h"),
      os.path.join(os.path.sep, "usr", "include", "hdfs.h"),
      )
    if fname:
      dir, name = os.path.split(fname)
      if dir != os.path.join(os.path.sep, "usr", "include"):
        self.hdfs_inc_path = dir
      else:
        self.hdfs_inc_path = ""
    else:
      raise RuntimeError("Couldn't find hdfs.h in source directory or /usr/include.")

  def __init_paths(self):
    self.java_home = os.getenv("JAVA_HOME", find_first_existing("/opt/sun-jdk", "/usr/lib/jvm/java-6-sun"))
    if self.java_home is None:
      raise RuntimeError("Could not determine JAVA_HOME path")

    self.hadoop_home = os.getenv("HADOOP_HOME", find_first_existing("/opt/hadoop", "/usr/lib/hadoop"))
    if self.hadoop_home is None:
      raise RuntimeError("Could not determine HADOOP_HOME path")

    # Set the "HADOOP_VERSION" env var if this fails
    self.hadoop_version = get_hadoop_version(self.hadoop_home)

    self.src = self.__find_hadoop_src()
    if not self.src:
      raise RuntimeError("Couldn't find Hadoop source code.  Please specify a path through the HADOOP_SRC environment variable, or provide HADOOP_HOME with a 'src' directory under it.")

    self.mapred_src = os.path.join(self.src, "c++")
    if not os.path.exists(self.mapred_src):
      raise RuntimeError("Hadoop source directory %s doesn't contain a 'c++' subdirectory.  If the source directory path is correct, please report a bug" % self.src)
    self.__set_mapred_inc_paths()
    self.__set_hdfs_inc_path()
    self.__set_hdfs_link_paths()

    print "========================================="
    print "paths:"
    names = ("hadoop_home", "hadoop_version", "src", "mapred_src", "mapred_inc", "hdfs_inc_path", "hdfs_link_paths",)
    for n in names:
      print n, ": ", getattr(self, n)
    print "========================================="

######################### main ################################

mtime = lambda fn: os.stat(fn).st_mtime


# https://issues.apache.org/jira/browse/MAPREDUCE-1125
OLD_DESERIALIZE_FLOAT = """void deserializeFloat(float& t, InStream& stream)
  {
    char buf[sizeof(float)];
    stream.read(buf, sizeof(float));
    XDR xdrs;
    xdrmem_create(&xdrs, buf, sizeof(float), XDR_DECODE);
    xdr_float(&xdrs, &t);
  }"""
NEW_DESERIALIZE_FLOAT = """float deserializeFloat(InStream& stream)
  {
    float t;
    char buf[sizeof(float)];
    stream.read(buf, sizeof(float));
    XDR xdrs;
    xdrmem_create(&xdrs, buf, sizeof(float), XDR_DECODE);
    xdr_float(&xdrs, &t);
    return t;
  }"""

# Ticket #250
OLD_WRITE_BUFFER =r"""void writeBuffer(const string& buffer) {
      fprintf(stream, quoteString(buffer, "\t\n").c_str());
    }"""
NEW_WRITE_BUFFER =r"""void writeBuffer(const string& buffer) {
      fprintf(stream, "%s", quoteString(buffer, "\t\n").c_str());
    }"""

setup(
  name="pydoop",
  version=pydoop.__version__,
  description=pydoop.__doc__.strip().splitlines()[0],
  long_description=pydoop.__doc__.lstrip(),
  author=pydoop.__author__,
  author_email=pydoop.__author_email__,
  url=pydoop.__url__,
  download_url="https://sourceforge.net/projects/pydoop/files/",
  packages=["pydoop"],
  cmdclass={"build_ext": build_pydoop_ext},
  ext_modules=create_ext_modules(),
  platforms=["Linux"],
  license="Apache-2.0",
  keywords=["hadoop", "mapreduce"],
  classifiers=[
    "Programming Language :: Python",
    "License :: OSI Approved :: Apache Software License",
    "Operating System :: POSIX :: Linux",
    "Topic :: Software Development :: Libraries :: Application Frameworks",
    "Intended Audience :: Developers",
    ],
  )

# vim: set sw=2 ts=2 et
