#!/bin/bash

OS=$(uname -s)

if [ "$OS" = "Linux" ]; then
    echo "This is a Linux system."
    pip3 install pyarrow
    export LD_LIBRARY_PATH=$(python -c 'import pyarrow; print(pyarrow.__path__[0])')
    auditwheel repair -w {dest_dir} {wheel}
elif [ "$OS" = "Darwin" ]; then
    echo "Attempting Mac"
    pip3 install pyarrow
    export DYLD_LIBRARY_PATH=$(python -c 'import pyarrow; print(pyarrow.__path__[0])')
    delocate-wheel --require-archs {delocate_archs} -w {dest_dir} -v {wheel}
else
    echo "Unknown operating system."
fi



