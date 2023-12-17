# Variables
CC = gcc
CXX = g++
ARROW=/home/tony/miniconda3/envs/dataviz/lib/python3.9/site-packages/pyarrow
PYTHON=/home/tony/miniconda3/envs/dataviz
PYTHON_VERSION=3.9
CFLAGS = -pthread -B /home/ziheng/miniconda3/envs/dataviz/compiler_compat -Wl,--sysroot=/ -Wsign-compare -DNDEBUG -O3 -g -fwrapv -Wall -Wstrict-prototypes -fPIC -D_GLIBCXX_USE_CXX11_ABI=0  -I$(ARROW)/include -I$(PYTHON)/include/python$(PYTHON_VERSION) -std=c++17
LDFLAGS = -pthread -shared -B $(PYTHON)/compiler_compat -L$(PYTHON)/lib -Wl,-rpath=$(PYTHON)/lib -Wl,--no-as-needed -Wl,--sysroot=/
LIBS = -L$(ARROW) -l:libarrow.so.1400 -l:libarrow_python.so -l:libpython$(PYTHON_VERSION).so -lsqlite3

# Targets
all: nfa.cpython-37m-x86_64-linux-gnu.so interval_nfa.cpython-37m-x86_64-linux-gpu.so

nfa.o: nfa_cep.cpp
	$(CC) $(CFLAGS) -c $< -o $@

interval_nfa.o: interval_nfa_cep.cpp
	$(CC) $(CFLAGS) -c $< -o $@

nfa.cpython-37m-x86_64-linux-gnu.so: nfa.o
	$(CXX) $(LDFLAGS) $^ $(LIBS) -o $@

interval_nfa.cpython-37m-x86_64-linux-gpu.so: interval_nfa.o
	$(CXX) $(LDFLAGS) $^ $(LIBS) -o $@

clean:
	rm -f *.o *.so

PHONY: all clean
