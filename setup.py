# BEGIN_COPYRIGHT
# END_COPYRIGHT

import sys, os, platform, re
from distutils.core import setup
from distutils.extension import Extension
from distutils.command.build_ext import build_ext

import pydoop
from pydoop.hadoop_utils import get_hadoop_version

# These variables MUST point to the correct locations, see installation docs
JAVA_HOME = os.getenv("JAVA_HOME") or "/opt/sun-jdk"
HADOOP_HOME = os.getenv("HADOOP_HOME") or "/opt/hadoop"

# This is optional: in most cases, get_hadoop_version() should work fine
HADOOP_VERSION = os.getenv("HADOOP_VERSION") or get_hadoop_version(HADOOP_HOME)

MAPRED_SRC = HDFS_SRC = "src/c++"
if HADOOP_VERSION >= (0,21,0):
    MAPRED_SRC = os.path.join("mapred", MAPRED_SRC)
    HDFS_SRC = os.path.join("hdfs", HDFS_SRC)
MAPRED_SRC = os.path.join(HADOOP_HOME, MAPRED_SRC)
HDFS_SRC = os.path.join(HADOOP_HOME, HDFS_SRC)


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


def get_hadoop_include_dirs(hadoop_home):
    a = "-".join(get_arch())
    return [os.path.join(hadoop_home, "c++/Linux-%s/include" % a)]


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

    def generate_main(self):
        sys.stderr.write("generating main for %s...\n" % self.name)
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
        destdir = os.path.split(self.wrap_sources[0])[0]  # should be ok
        outfn = os.path.join(destdir, "%s_main.cpp" % self.module_name)
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
            f = open(fn)
            contents = f.read()
            f.close()
            for old, new in self.patches[fn].iteritems():
                contents = contents.replace(old, new)
            patched_fn = "src/%s" % os.path.basename(fn)
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


def create_pipes_ext():
    wrap = ["pipes", "pipes_context", "pipes_test_support",
            "pipes_serial_utils", "exceptions", "pipes_input_split"]
    aux = []
    basedir = MAPRED_SRC
    patches = {
        os.path.join(basedir, "utils/impl/SerialUtils.cc"): {
            '#include "hadoop/SerialUtils.hh"':
            '#include <stdint.h>\n#include "hadoop/SerialUtils.hh"',
            "#include <string>": "#include <string.h>",
            OLD_DESERIALIZE_FLOAT: NEW_DESERIALIZE_FLOAT
            },
        os.path.join(basedir, "utils/impl/StringUtils.cc"): {
            "#include <strings.h>": "#include <string.h>\n#include <stdlib.h>"
            },
        os.path.join(basedir, "pipes/impl/HadoopPipes.cc"): {
            '#include "hadoop/Pipes.hh"':
            '#include <stdint.h>\n#include "hadoop/Pipes.hh"',
            "#include <strings.h>": "#include <string.h>",
            OLD_WRITE_BUFFER: NEW_WRITE_BUFFER
            },
        }
    return BoostExtension(
        "pydoop._pipes",
        ["src/%s.cpp" % n for n in wrap],
        ["src/%s.cpp" % n for n in aux],
        patches=patches,
        include_dirs=get_hadoop_include_dirs(HADOOP_HOME),
        libraries = ["pthread", "boost_python"],
        define_macros=get_pipes_macros(HADOOP_VERSION)
        )


def create_hdfs_ext():
    wrap = ["hdfs_fs", "hdfs_file", "hdfs_common"]
    aux = []
    library_dirs = get_java_library_dirs(JAVA_HOME) + [
        os.path.join(HADOOP_HOME, "c++/Linux-%s-%s/lib" % get_arch())
        ]
    hdfs_include_dir = os.path.join(HDFS_SRC, "libhdfs")
    return BoostExtension(
        "pydoop._hdfs",
        ["src/%s.cpp" % n for n in wrap],
        ["src/%s.cpp" % n for n in aux],
        include_dirs=get_java_include_dirs(JAVA_HOME) + [hdfs_include_dir],
        library_dirs=library_dirs,
        runtime_library_dirs=library_dirs,
        libraries=["pthread", "boost_python", "hdfs", "jvm"],
        define_macros=get_hdfs_macros(os.path.join(hdfs_include_dir, "hdfs.h"))
        )
    return factory.create()


def create_ext_modules():
    ext_modules = []
    ext_modules.append(create_pipes_ext())
    ext_modules.append(create_hdfs_ext())
    return ext_modules


setup(
    name="pydoop",
    version=pydoop.__version__,
    description=pydoop.__doc__.strip().splitlines()[0],
    author=pydoop.__author__,
    author_email=pydoop.__author_email__,
    url=pydoop.__url__,
    packages=["pydoop"],
    cmdclass={"build_ext": build_boost_ext},
    ext_modules=create_ext_modules(),
    platforms=["linux"],
    license="Apache-2.0"
    )
