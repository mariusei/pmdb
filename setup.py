from distutils.core import setup, Extension, DEBUG

pmdb_module = Extension('pmdb', sources = ['pmdb.cpp'])

setup(name = 'pmdb', version = '1.0',
    description = 'Persistent Memory Database - Python extension',
    ext_modules = [pmdb_module]
    )
