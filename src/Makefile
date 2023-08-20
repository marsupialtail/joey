# Variables
CC = gcc
CXX = g++
ARROW=/home/ziheng/miniconda3/envs/quokka-dev/lib/python3.8/site-packages/pyarrow
PYTHON=/home/ziheng/miniconda3/envs/quokka-dev
CFLAGS = -pthread -B /home/ziheng/miniconda3/envs/quokka-dev/compiler_compat -Wl,--sysroot=/ -Wsign-compare -DNDEBUG -O3 -fwrapv -Wall -Wstrict-prototypes -fPIC -D_GLIBCXX_USE_CXX11_ABI=0  -I$(ARROW)/include -I$(PYTHON)/include/python3.8 -std=c++17
LDFLAGS = -pthread -shared -B $(PYTHON)/compiler_compat -L$(PYTHON)/lib -Wl,-rpath=$(PYTHON)/lib -Wl,--no-as-needed -Wl,--sysroot=/
LIBS = -L$(ARROW) -l:libarrow.so.1200 -l:libarrow_python.so -l:libpython3.8.so -lsqlite3

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

.PHONY: all clean
