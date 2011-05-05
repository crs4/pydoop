# BEGIN_COPYRIGHT
# END_COPYRIGHT

import sys, os, platform, re
from distutils.core import setup
from distutils.extension import Extension
from distutils.command.build_ext import build_ext

import pydoop
from pydoop.hadoop_utils import get_hadoop_version

# https://issues.apache.org/jira/browse/MAPREDUCE-375 -- integrated in 0.21.0
def get_pipes_macros(hadoop_version):
  pipes_macros = []
  if hadoop_version >= (0,21,0):
    pipes_macros.append(("VINT_ISPLIT_FILENAME", None))
  return pipes_macros


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


class build_boost_ext(build_ext):

  def finalize_options(self):
    build_ext.finalize_options(self)
    for e in self.extensions:
      e.sources.append(e.generate_main())
      e.sources.extend(e.generate_patched_aux())


def create_pipes_ext(path_finder):
  wrap = ["pipes", "pipes_context", "pipes_test_support",
          "pipes_serial_utils", "exceptions", "pipes_input_split"]
  aux = []
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
  return BoostExtension(
    "pydoop._pipes",
    ["src/%s.cpp" % n for n in wrap],
    ["src/%s.cpp" % n for n in aux],
    patches=patches,
    include_dirs=path_finder.mapred_inc,
    libraries = ["pthread", "boost_python"],
    define_macros=get_pipes_macros(path_finder.hadoop_version)
    )


def create_hdfs_ext(path_finder):
  wrap = ["hdfs_fs", "hdfs_file", "hdfs_common"]
  aux = []
  library_dirs = get_java_library_dirs(JAVA_HOME) + path_finder.hdfs_link_paths["L"]
  return BoostExtension(
    "pydoop._hdfs",
    ["src/%s.cpp" % n for n in wrap],
    ["src/%s.cpp" % n for n in aux],
    include_dirs=get_java_include_dirs(JAVA_HOME) + [path_finder.hdfs_inc_path],
    library_dirs=library_dirs,
    runtime_library_dirs=library_dirs,
    libraries=["pthread", "boost_python", "hdfs", "jvm"],
    define_macros=get_hdfs_macros(os.path.join(path_finder.hdfs_inc_path, "hdfs.h"))
    )
  return factory.create()


def create_ext_modules(path_finder):
  ext_modules = []
  ext_modules.append(create_pipes_ext(path_finder))
  ext_modules.append(create_hdfs_ext(path_finder))
  return ext_modules

def find_first_existing(*paths):
  for p in paths:
    if os.path.exists(p):
      return p
  return None

class PathFinder(object):
  def __init__(self, hadoop_home, hadoop_ver):
    self.home = hadoop_home
    self.hadoop_version = hadoop_ver
    self.src = None
    self.mapred_src = None
    self.mapred_inc = []
    self.hdfs_inc_path = None # special case, only one include path since we only have one file
    self.hdfs_link_paths = { "L":[], "l":[] }

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
    if self.home:
      if os.path.exists( os.path.join(self.home, "src") ):
        return os.path.join(self.home, "src")

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
    if os.getenv("HADOOP_INC_PATH"):
      self.mapred_inc = os.getenv("HADOOP_INC_PATH").split(os.pathsep)
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
      os.path.join(self.home, "hdfs", "c++", "Linux-%s-%s" % get_arch(), "lib", "libhdfs.so"),
      os.path.join(self.home, "c++", "Linux-%s-%s" % get_arch(), "lib", "libhdfs.so"),
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

  def init_paths(self):
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
    names = ("home", "hadoop_version", "src", "mapred_src", "mapred_inc", "hdfs_inc_path", "hdfs_link_paths",)
    for n in names:
      print n, ": ", getattr(self, n)

 



######################### main ################################

JAVA_HOME = os.getenv("JAVA_HOME", find_first_existing("/opt/sun-jdk", "/usr/lib/jvm/java-6-sun"))
HADOOP_HOME = os.getenv("HADOOP_HOME", find_first_existing("/opt/hadoop", "/usr/lib/hadoop"))
# Set the "HADOOP_VERSION" env var if this fails
HADOOP_VERSION = get_hadoop_version(HADOOP_HOME)

path_finder = PathFinder(HADOOP_HOME, HADOOP_VERSION)
path_finder.init_paths()

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
  cmdclass={"build_ext": build_boost_ext},
  ext_modules=create_ext_modules(path_finder),
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
