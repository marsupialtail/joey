import os, sys
from setuptools import setup, find_packages, Extension
import pyarrow
os.environ["CC"] = "g++"
os.environ["CXX"] = "g++"

# Define the paths for Arrow and Python
ARROW_PATH = pyarrow.__file__.replace("/__init__.py", "")
PYTHON_PATH = sys.executable.replace("/bin/python3", "")
PYTHON_VERSION = f"{sys.version_info.major}.{sys.version_info.minor}"

# Compiler and linker flags
extra_compile_args = ['-O3', '-g', '-fPIC', '-std=c++17', f'-I{ARROW_PATH}/include/', f'-I{PYTHON_PATH}/include/python{PYTHON_VERSION}']
extra_link_args = ['-shared', f'-B{PYTHON_PATH}/compiler_compat', f'-L{PYTHON_PATH}/lib', '-Wl,-rpath=' + PYTHON_PATH]
libraries = [':libarrow.so', ':libarrow_python.so', f':libpython{PYTHON_VERSION}.so', 'sqlite3']

# Define the extension modules
extensions = [
    Extension('nfa', 
              sources=['src/nfa_cep.cpp'],
              extra_compile_args=extra_compile_args,
              extra_link_args=extra_link_args,
              libraries=libraries,
              library_dirs=[ARROW_PATH, PYTHON_PATH + '/lib']),
    Extension('interval_nfa', 
              sources=['src/interval_nfa_cep.cpp'],
              extra_compile_args=extra_compile_args,
              extra_link_args=extra_link_args,
              libraries=libraries,
              library_dirs=[ARROW_PATH, PYTHON_PATH + '/lib']),
    Extension('interval_vector', 
              sources=['src/interval_vector_cep.cpp'],
              extra_compile_args=extra_compile_args,
              extra_link_args=extra_link_args,
              libraries=libraries,
              library_dirs=[ARROW_PATH, PYTHON_PATH + '/lib']),
    Extension('interval_dfs', 
              sources=['src/interval_dfs_cep.cpp'],
              extra_compile_args=extra_compile_args,
              extra_link_args=extra_link_args,
              libraries=libraries,
              library_dirs=[ARROW_PATH, PYTHON_PATH + '/lib'])
]
setup(
    name='pyjoey',
    version='0.2.3',
    author='Tony Wang',
    author_email='zihengw@stanford.edu',
    description='Event analytics. Very fast. Will eventually be merged into Quokka',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    packages=find_packages(),  # This will find the 'pyjoey' package in your project root
    install_requires=[
        'duckdb',
        'pyarrow>=7.0.0',
        'sqlglot>=17.8.0',
        'tqdm>=4.65.0',
        'polars>=0.18.0'
    ],
    # Add ext_modules if you have extensions
    ext_modules=extensions
)
