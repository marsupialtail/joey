import os, re, sys
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
extra_link_args = [f'-L{PYTHON_LIB_DIR}', '-L{ARROW_PATH}', '-Wl,-rpath,' + PYTHON_LIB_DIR]
extra_link_args += ['-larrow', '-larrow_python', '-lsqlite3']

# Pyarrow distributions contain the shared libraries that we want to link with.  However, they are not named
# consistently.  Some libraries are named .(so|dylib) and some are named .1400.(so|dylib) where 1400 is the
# version number.  In order to link to these latter files we need to create symlinks named .(so|dylib).
for file in os.listdir(ARROW_PATH):
    if re.search("\\.\\d\\d\\d\\d\\.dylib", file):
        short_name = file[:-11] + ".dylib"
    elif re.search("\\.\\d\\d\\d\\d\\.so", file):
        short_name = file[:-8] + ".so"
    else:
        short_name = None
    if short_name:
        print(f"Creating symlink from {ARROW_PATH}/{file} TO {ARROW_PATH}/{short_name}")
        if not os.path.exists(f"{ARROW_PATH}/{short_name}"):
            print(f"Skipping symlink from {ARROW_PATH}/{file} because f{ARROW_PATH}/{short_name} exists")
            os.symlink(f"{ARROW_PATH}/{file}", f"{ARROW_PATH}/{short_name}")
    else:
        print(f"Ignoring {file}")

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
