import os, sys
from setuptools import setup, find_packages, Extension
import distutils.sysconfig as sysconfig
import pyarrow

os.environ["CC"] = "g++"
os.environ["CXX"] = "g++"

# Define the paths for Arrow and Python
ARROW_PATH = pyarrow.__file__.replace("/__init__.py", "")
PYTHON_INCLUDE_DIR = sysconfig.get_python_inc()
PYTHON_LIB_DIR = sysconfig.get_config_var('LIBDIR')
PYTHON_VERSION = f"{sys.version_info.major}.{sys.version_info.minor}"

# Compiler and linker flags
extra_compile_args = ['-O3', '-g', '-fPIC', '-std=c++17', f'-I{ARROW_PATH}/include/', f'-I{PYTHON_INCLUDE_DIR}']
extra_link_args = [f'-L{PYTHON_LIB_DIR}', '-Wl,-rpath=' + PYTHON_LIB_DIR]
extra_link_args += ['-l:libarrow.so.1400', '-l:libarrow_python.so', '-lsqlite3']
# extra_link_args += ['-l:libarrow.so.1400', '-lsqlite3']

# Define the extension modules
extensions = [
    Extension('pyjoey.nfa', 
              sources=['src/nfa_cep.cpp'],
              extra_compile_args=extra_compile_args,
              extra_link_args=extra_link_args,
              library_dirs=[ARROW_PATH, PYTHON_LIB_DIR]),
    Extension('pyjoey.interval_nfa', 
              sources=['src/interval_nfa_cep.cpp'],
              extra_compile_args=extra_compile_args,
              extra_link_args=extra_link_args,
              library_dirs=[ARROW_PATH, PYTHON_LIB_DIR]),
    Extension('pyjoey.interval_vector', 
              sources=['src/interval_vector_cep.cpp'],
              extra_compile_args=extra_compile_args,
              extra_link_args=extra_link_args,
              library_dirs=[ARROW_PATH, PYTHON_LIB_DIR]),
    Extension('pyjoey.interval_dfs', 
              sources=['src/interval_dfs_cep.cpp'],
              extra_compile_args=extra_compile_args,
              extra_link_args=extra_link_args,
              library_dirs=[ARROW_PATH, PYTHON_LIB_DIR])
]
setup(
    name='pyjoey',
    version='0.2.3',
    author='Tony Wang',
    author_email='zihengw@stanford.edu',
    description='Event analytics. Very fast. Will eventually be merged into Quokka',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    packages=['pyjoey'],  # This will find the 'pyjoey' package in your project root
    package_data = {'pyjoey': ['nfa', 'interval_nfa', 'interval_vector', 'interval_dfs']}, 
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
