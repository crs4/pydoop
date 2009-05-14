import sys, os, platform
from distutils.core import setup, Extension


def get_arch():
    bits, linkage = platform.architecture()
    if bits == "64bit":
        return "amd64-64"
    return "i386-32"


def get_ext_modules():
    cpp_names = [
        "HadoopPipes",
        "SerialUtils",
        "StringUtils",
        "hacked_wrapper",
        "hadoop_pipes",
        "hadoop_pipes_context",
        "hadoop_pipes_test_support",
        "hadoop_pipes_main",  # auto-generated in original Makefile
        ]
    cpp_files = ["src/%s.cpp" % n for n in cpp_names]
    include_dirs = ["/opt/hadoop/c++/Linux-%s/include" % get_arch()]
    library_dirs = ["/opt/hadoop/c++/Linux-%s/lib" % get_arch()]
    libnames = []
    hadoop_libs = ["hadoop" + n for n in libnames]
    other_libs = ["pthread"]
    libraries = hadoop_libs + other_libs + ["boost_python"]
    ext = Extension(
        "hadoop_pipes",
        cpp_files,
        include_dirs=include_dirs,
        library_dirs=library_dirs,
        libraries=libraries,
        extra_compile_args=["-O3"]
        )
    return [ext]


setup(
    name="pydoop",
    version = "0.1.1",
    description="Python MapReduce API for Hadoop",
    author = "Gianluigi Zanetti",
    author_email = "<gianluigi.zanetti@crs4.it>",
    maintainer = "Simone Leo",
    maintainer_email = "simleo@crs4.it",
    url = "http://svn.crs4.it/ac-dc/lib/pydoop",
    py_modules = ["pydoop"],
    ext_modules=get_ext_modules(),
    )
