# Variables
CC = gcc
CXX = g++
CFLAGS = -pthread -B /home/ziheng/miniconda3/envs/quokka-dev/compiler_compat -Wl,--sysroot=/ -Wsign-compare -DNDEBUG -g -O3 -fwrapv -Wall -Wstrict-prototypes -fPIC -D_GLIBCXX_USE_CXX11_ABI=0 -I/home/ziheng/miniconda3/envs/quokka-dev/lib/python3.8/site-packages/numpy/core/include -I/home/ziheng/miniconda3/envs/quokka-dev/lib/python3.8/site-packages/pyarrow/include -I/home/ziheng/miniconda3/envs/quokka-dev/include/python3.8 -std=c++17
LDFLAGS = -pthread -shared -B /home/ziheng/miniconda3/envs/quokka-dev/compiler_compat -L/home/ziheng/miniconda3/envs/quokka-dev/lib -Wl,-rpath=/home/ziheng/miniconda3/envs/quokka-dev/lib -Wl,--no-as-needed -Wl,--sysroot=/
LIBS = -L/home/ziheng/miniconda3/envs/quokka-dev/lib/python3.8/site-packages/pyarrow -l:libarrow.so.1200 -l:libarrow_python.so -l:libpython3.8.so -lsqlite3

# Targets
all: example.cpython-37m-x86_64-linux-gnu.so

example.o: interval_nfa_cep.cpp
	$(CC) $(CFLAGS) -c $< -o $@

example.cpython-37m-x86_64-linux-gnu.so: example.o
	$(CXX) $(LDFLAGS) $^ $(LIBS) -o $@

clean:
	rm -f example.o example.cpython-37m-x86_64-linux-gnu.so

.PHONY: all clean
