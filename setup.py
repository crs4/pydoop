import sys, os, platform, re
from distutils.core import setup, Extension
from distutils.command.build_clib import *


# JAVA_HOME=my/java/home HADOOP_HOME=my/hadoop/home python setup.py build
JAVA_HOME = os.getenv("JAVA_HOME") or "/opt/sun-jdk"

HADOOP_HOME = os.getenv("HADOOP_HOME") or "/opt/hadoop"


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


class BoostExtFactory(object):

    export_pattern = re.compile(r"void\s+export_(\w+)")

    def __init__(self, name, wrap_files, aux_files, **ext_args):
        self.name = name
        self.wrap_files = wrap_files
        self.aux_files = aux_files
        self.main = self.__generate_main()
        self.ext_args = ext_args

    def __generate_main(self):
        sys.stderr.write("generating main for %s...\n" % self.name)
        first_half = ["#include <boost/python.hpp>"]
        second_half = ["BOOST_PYTHON_MODULE(%s){" % self.name]
        for fn in self.wrap_files:
            f = open(fn)
            code = f.read()
            f.close()
            m = self.export_pattern.search(code)
            if m is not None:
                fun_name = "export_%s" % m.groups()[0]
                first_half.append("void %s();" % fun_name)
                second_half.append("%s();" % fun_name)
        second_half.append("}")
        destdir = os.path.split(self.wrap_files[0])[0]  # should be fine
        outfn = os.path.join(destdir, "%s_main.cpp" % self.name)
        outf = open(outfn, "w")
        for line in first_half:
            outf.write("%s%s" % (line, os.linesep))
        for line in second_half:
            outf.write("%s%s" % (line, os.linesep))
        outf.close()
        return outfn

    def create(self):
        all_files = self.aux_files + self.wrap_files + [self.main]
        return Extension(self.name, all_files, **self.ext_args)


class pydoop_build_clib(build_clib):

    def build_libraries (self, libraries):  # cut-n-paste
        for (lib_name, build_info) in libraries:
            sources = build_info.get('sources')
            if sources is None or type(sources) not in (ListType, TupleType):
                raise DistutilsSetupError, \
                      ("in 'libraries' option (library '%s'), " +
                       "'sources' must be present and must be " +
                       "a list of source filenames") % lib_name
            sources = list(sources)
            log.info("building '%s' library", lib_name)
            macros = build_info.get('macros')
            include_dirs = build_info.get('include_dirs')
            objects = self.compiler.compile(sources,
                                            output_dir=self.build_temp,
                                            macros=macros,
                                            include_dirs=include_dirs,
                                            debug=self.debug)
            # here comes the hack
            objects.append(os.path.join(get_java_library_dirs(JAVA_HOME)[0],
                                        "libjvm.so"))
            self.compiler.create_static_lib(objects, lib_name,
                                            output_dir=self.build_clib,
                                            debug=self.debug)


def create_pipes_ext():
    wrap = ["pipes", "pipes_context", "pipes_test_support", "exceptions"]
    aux = ["HadoopPipes", "SerialUtils", "StringUtils"]
    factory = BoostExtFactory(
        "pydoop_pipes",
        ["src/%s.cpp" % n for n in wrap],
        ["src/%s.cpp" % n for n in aux],
        include_dirs=get_hadoop_include_dirs(HADOOP_HOME),
        libraries = ["pthread", "boost_python"],
        )
    return factory.create()


def create_hdfs_ext():
    wrap = ["hdfs_fs", "hdfs_file", "hdfs_common"]
    aux = []
    factory = BoostExtFactory(
        "pydoop_hdfs",
        ["src/%s.cpp" % n for n in wrap],
        ["src/%s.cpp" % n for n in aux],
        include_dirs=get_java_include_dirs(JAVA_HOME) + ["src/libhdfs"],
        runtime_library_dirs=get_java_library_dirs(JAVA_HOME),
        libraries=["pthread", "boost_python", "hdfs"],
        )
    return factory.create()


def create_libhdfs_clib():
    name = "hdfs"
    src = ["hdfs", "hdfsJniHelper"]
    build_info = {"sources": ["src/libhdfs/%s.c" % s for s in src],
                  "include_dirs": get_java_include_dirs(JAVA_HOME),}
    return (name, build_info)


def create_ext_modules():
    ext_modules = []
    ext_modules.append(create_pipes_ext())
    ext_modules.append(create_hdfs_ext())
    return ext_modules


setup(
    name="pydoop",
    version="0.2.4",
    description="Python MapReduce API for Hadoop",
    author="Gianluigi Zanetti",
    author_email="<gianluigi.zanetti@crs4.it>",
    maintainer="Simone Leo",
    maintainer_email="simleo@crs4.it",
    url="http://svn.crs4.it/ac-dc/lib/pydoop",
    packages=["pydoop"],
    libraries=[create_libhdfs_clib()],
    ext_modules=create_ext_modules(),
    cmdclass={"build_clib": pydoop_build_clib,},
    )
