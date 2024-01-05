#!/bin/bash
PYARROW_PATH=$(python -c 'import pyarrow; print(pyarrow.__path__[0])')
# Export the updated LD_LIBRARY_PATH
export LD_LIBRARY_PATH=$PYARROW_PATH:$LD_LIBRARY_PATH
echo "Updated LD_LIBRARY_PATH to $LD_LIBRARY_PATH"
