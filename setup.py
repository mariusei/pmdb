from distutils.core import setup, Extension, DEBUG
import os

os.environ["CC"] = "g++"
os.environ["CXX"] = "g++"

pmdb_module = Extension('pmdb',
        extra_compile_args=['-std=c++11'],
        extra_link_args=['-lpmemobj'],
        sources = ['pmdb.cpp'])

setup(name = 'pmdb', version = '1.0',
    description = 'Persistent Memory Database - Python extension',
    ext_modules = [pmdb_module]
    )
