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

import sys, os, platform, re, glob, shutil
from distutils.core import setup
from distutils.extension import Extension
from distutils.command.build_ext import build_ext as distutils_build_ext
from distutils.command.clean import clean as distutils_clean
from distutils.command.build import build as distutils_build
from distutils.errors import DistutilsSetupError
from distutils import log

import pydoop
import pydoop.hadoop_utils as hadoop_utils

PipesSrc = ["pipes", "pipes_context", "pipes_test_support",
          "pipes_serial_utils", "exceptions", "pipes_input_split"]
HdfsSrc = ["hdfs_fs", "hdfs_file", "hdfs_common"]
PipesExtName = "_pipes"
HdfsExtName = "_hdfs"

###############################################################################
# Utility functions
###############################################################################
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

# Given a list of paths, returns the first that exists.
def find_first_existing(*paths):
  for p in paths:
    if os.path.exists(p):
      return p
  return None

###############################################################################
# Create extension objects.
#
# We first create some basic Extension objects to pass to the distutils setup
# function.  The act as little more than placeholders, simply telling distutils
# the name of the extension and what source files it depends on.
#   functions:  create_basic_(pipes|hdfs)_ext
#
# When our build_pydoop_ext command is invoked, we build a complete extension
# object that includes all the information required for the build process.  In
# particular, it includes all the relevant paths.
#
# The reason for the two-stage process is to delay verifying paths to when 
# they're needed (build) and avoiding those checks for other commands (such
# as clean)..
###############################################################################
def create_basic_pipes_ext():
  return BoostExtension(PipesExtName, ["src/%s.cpp" % n for n in PipesSrc], [])

def create_basic_hdfs_ext():
  return BoostExtension(HdfsExtName, ["src/%s.cpp" % n for n in HdfsSrc], [])

def create_full_pipes_ext(path_finder):
  basedir = path_finder.mapred_src
  serial_utils_cc = os.path.join(basedir, "utils/impl/SerialUtils.cc")
  pipes_cc = os.path.join(basedir, "pipes/impl/HadoopPipes.cc")
  patches = {
    serial_utils_cc: {
      OLD_DESERIALIZE_FLOAT: NEW_DESERIALIZE_FLOAT
      },
    os.path.join(basedir, "utils/impl/StringUtils.cc"): {
      },
    pipes_cc: {
      OLD_WRITE_BUFFER: NEW_WRITE_BUFFER
      },
    }
  include_dirs = path_finder.mapred_inc
  libraries = ["pthread", "boost_python"]
  if path_finder.hadoop_version()[2] == 203 or path_finder.cloudera():
    include_dirs.append("/usr/include/openssl")
    libraries.append("ssl")
    patches[serial_utils_cc][OLD_SERIAL_UTILS_INCLUDE] = NEW_SERIAL_UTILS_INCLUDE
    patches[pipes_cc][OLD_PIPES_CC_INCLUDE] = NEW_PIPES_CC_INCLUDE
  return BoostExtension(
    pydoop.complete_mod_name(PipesExtName, path_finder.hadoop_version()),
    ["src/%s.cpp" % n for n in PipesSrc],
    [], # aux
    patches=patches,
    include_dirs=include_dirs,
    libraries=libraries
    )

def create_full_hdfs_ext(path_finder):
  library_dirs = get_java_library_dirs(path_finder.java_home) + path_finder.hdfs_link_paths["L"]
  return BoostExtension(
    pydoop.complete_mod_name(HdfsExtName, path_finder.hadoop_version()),
    ["src/%s.cpp" % n for n in HdfsSrc],
    [], # aux
    include_dirs=get_java_include_dirs(path_finder.java_home) + [path_finder.hdfs_inc_path],
    library_dirs=library_dirs,
    runtime_library_dirs=library_dirs,
    libraries=["pthread", "boost_python", "hdfs", "jvm"],
    define_macros=get_hdfs_macros(os.path.join(path_finder.hdfs_inc_path, "hdfs.h"))
    )


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

###############################################################################
# Custom distutils extension and commands
###############################################################################

# Customized Extension class that generates the necessary Boost Python
# export code.
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

# Customized distutils build_ext command that sets the options
# required to build the Pydoop extensions.
class build_pydoop_ext(distutils_build_ext):
  def finalize_options(self):
    distutils_build_ext.finalize_options(self)

    path_finder = SetupPathFinder()

    self.extensions = [ create_full_pipes_ext(path_finder), create_full_hdfs_ext(path_finder) ]

    for e in self.extensions:
      e.sources.append(e.generate_main())
      e.sources.extend(e.generate_patched_aux())

def create_ext_modules():
  ext_modules = []
  ext_modules.append(create_basic_pipes_ext())
  ext_modules.append(create_basic_hdfs_ext())
  return ext_modules


# Custom clean action that removes files generated by the build process.
# In particular, the build process generates _*_main.cpp files for the boost
# extensions, and some patched Hadoop source code files, all inside the src
# directory.  These are removed when this clean action is executed.
class pydoop_clean(distutils_clean):
  def run(self):
    distutils_clean.run(self)
    shutil.rmtree(os.path.join(os.path.dirname(os.path.realpath(__file__)), 'dist'), ignore_errors=True)
    pydoop_src_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'src')
    ## remove generated files and patched Hadoop pipes code 
    r = re.compile('(%s|%s)_.*_main.cpp$' % (HdfsExtName, PipesExtName))
    paths = filter(r.search, os.listdir(pydoop_src_path)) + ['SerialUtils.cc', 'StringUtils.cc', 'HadoopPipes.cc']
    absolute_paths = [ os.path.join(pydoop_src_path, f) for f in paths ]
    for f in absolute_paths:
      if not self.dry_run:
        try:
          if os.path.exists(f):
            os.remove(f)
        except OSError as e:
          print >>sys.stderr, "Error removing file.", e

class pydoop_build(distutils_build):
  def run(self):
    distutils_build.run(self)
    # build the java component
    classpath = ':'.join(
        glob.glob( os.path.join(pydoop.hadoop_home(), 'hadoop-*.jar') ) +
        glob.glob( os.path.join(pydoop.hadoop_home(), 'lib', '*.jar') ) )
    class_dir = os.path.join(self.build_temp, 'pydoop_java')
    package_path = os.path.join(self.build_lib, 'pydoop', pydoop.__jar_name__)

    if not os.path.exists(class_dir):
      os.mkdir(class_dir)
    compile_cmd = "javac -classpath %s -d '%s' src/it/crs4/pydoop/NoSeparatorTextOutputFormat.java" % (classpath, class_dir)
    package_cmd = "jar -cf %s -C %s ./it" % (package_path, class_dir)
    log.info("Compiling Java classes")
    log.debug("Command: %s", compile_cmd)
    ret = os.system(compile_cmd)
    if ret:
      raise DistutilsSetupError("Error compiling java component.  Command: %s" % compile_cmd)
    log.info("Packaging Java classes")
    log.debug("Command: %s", package_cmd)
    ret = os.system(package_cmd) 
    if ret:
      raise DistutilsSetupError("Error packaging java component.  Command: %s" % package_cmd)

###############################################################################
# Path finder
# Class that encapsulates the logic to find paths and other info required by
# the build process, such as:
# * Java path
# * Hadoop binary and source paths
# * Hadoop version
###############################################################################
class SetupPathFinder(hadoop_utils.PathFinder):
  def __init__(self):
    super(type(self), self).__init__() # call parent constructor
    self.java_home = None
    self.src = None
    self.mapred_src = None
    self.mapred_inc = []
    self.hdfs_inc_path = None # special case, only one include path since we only have one file
    self.hdfs_link_paths = { "L":[], "l":[] }
    self.__init_paths()

  # returns one of:
  #   1. HADOOP_SRC
  #   2. HADOOP_HOME/src
  #   3. /usr/src/hadoop*
  #   4. None
  # Returns the path if found
  def __find_hadoop_src(self):
    if os.getenv("HADOOP_SRC"):
      return os.getenv("HADOOP_SRC")
    if self.hadoop_home():
      if os.path.exists( os.path.join(self.hadoop_home(), "src") ):
        return os.path.join(self.hadoop_home(), "src")

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
      # we didn't find the expected include paths in the source.  Try the standard /usr/include/hadoop
      arch_string = "-".join(get_arch())
      candidate_paths = \
        glob.glob(os.path.join(self.src, "mapred", "c++", "Linux-%s" % arch_string, "include", "hadoop")) +\
        glob.glob(os.path.join(self.src, "c++", "Linux-%s" % arch_string, "include", "hadoop")) +\
        glob.glob(os.path.join( os.path.sep, "usr", "include", "hadoop*"))
      if candidate_paths:
        self.mapred_inc = [ os.path.dirname(candidate_paths[0]) ]
      else:
        raise RuntimeError("Couldn't find Hadoop c++ include directory.\nTry specifying one with HADOOP_INC_PATH")

  def __set_hdfs_link_paths(self):
    self.hdfs_link_paths["l"].append("hdfs") # link to libhdfs

    # But, where to find libhdfs?
    candidate_paths = \
      glob.glob(os.path.join(os.path.sep,"usr","lib*","libhdfs.so*")) + \
      glob.glob(os.path.join(self.hadoop_home(), "lib*", "libhdfs.so")) +\
      glob.glob(os.path.join(self.hadoop_home(), "hdfs", "c++", "Linux-%s-%s" % get_arch(), "lib", "libhdfs.so")) +\
      glob.glob(os.path.join(self.hadoop_home(), "c++", "Linux-%s-%s" % get_arch(), "lib", "libhdfs.so"))
    if candidate_paths: # glob only returns existing paths
      dir, name = os.path.split(candidate_paths[0])
      if dir != os.path.join(os.path.sep, "usr", "lib"):
        self.hdfs_link_paths["L"].append(dir)
    else:
      raise RuntimeError("Couldn't find libhdfs.so in HADOOP_HOME or /usr/lib.")
        
  def __set_hdfs_inc_path(self):
    candidate_paths = \
      glob.glob(os.path.join(os.path.sep, "usr","include","hdfs.h")) +\
      glob.glob(os.path.join(os.path.sep, "usr","include","hadoop*","hdfs.h")) +\
      glob.glob(os.path.join(self.src, "c++", "libhdfs", "hdfs.h"))
    if candidate_paths:
      dir, name = os.path.split(candidate_paths[0])
      self.hdfs_inc_path = dir
    else:
      raise RuntimeError("Couldn't find hdfs.h in source directory or /usr/include.")

  def __init_paths(self):
    # actually does not override the parent class' method since
    # they're both private methods
    self.java_home = os.getenv("JAVA_HOME", find_first_existing("/opt/sun-jdk", "/usr/lib/jvm/java-6-sun"))
    if self.java_home is None:
      raise RuntimeError("Could not determine JAVA_HOME path")

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
    names = ("java_home", "src", "mapred_src", "mapred_inc", "hdfs_inc_path", "hdfs_link_paths",)
    for n in names:
      print n, ": ", getattr(self, n)
    print "hadoop_home", self.hadoop_home()
    print "hadoop_version", self.hadoop_version()
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

# Pipes.hh and SerialUtils.hh don't include stdint.h.  Let's include it
# in HadoopPipes.cc before it includes the other headers
OLD_PIPES_CC_INCLUDE = """#include "hadoop/Pipes.hh"\n"""
NEW_PIPES_CC_INCLUDE = """#include <stdint.h>\n#include "hadoop/Pipes.hh"\n"""

OLD_SERIAL_UTILS_INCLUDE = """#include "hadoop/SerialUtils.hh"\n"""
NEW_SERIAL_UTILS_INCLUDE = """#include <stdint.h>\n#include "hadoop/SerialUtils.hh"\n"""

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
  cmdclass={'build': pydoop_build, "build_ext": build_pydoop_ext, 'clean': pydoop_clean},
  ext_modules=create_ext_modules(),
  scripts=["scripts/pydoop_script"],
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
