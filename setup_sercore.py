from distutils.core import setup, Extension


ext = Extension(
    "sercore",
    sources=[
        "src/sercore/sercore.cpp",
        "src/sercore/streams.cpp",
        "src/sercore/HadoopUtils/SerialUtils.cc",
        "src/sercore/HadoopUtils/StringUtils.cc"
    ],
    include_dirs=["src/sercore/HadoopUtils"],
    extra_compile_args=['-std=c++11'],
)

setup(
    name="sercore",
    version="2.0a3",
    ext_modules=[ext],
)
