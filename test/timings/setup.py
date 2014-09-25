from distutils.core import setup, Extension

module1 = Extension('dummy',
                    sources = ['dummy.cpp', 'SerialUtils.cc', 'StringUtils.cc'],
                    extra_compile_args=["-O3"])

setup (name = 'Dummy',
       version = '1.0',
       description = 'This is a dummy package',
       ext_modules = [module1])