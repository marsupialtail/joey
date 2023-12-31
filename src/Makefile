# Variables
CXX = g++
ARROW=/home/ziheng/miniconda3/envs/quokka-dev/lib/python3.8/site-packages/pyarrow
PYTHON=/home/ziheng/miniconda3/envs/quokka-dev
PYTHON_VERSION=3.8
CFLAGS = -O3 -g -fwrapv -Wall -Wstrict-prototypes -fPIC -I$(ARROW)/include/ -I$(PYTHON)/include/python$(PYTHON_VERSION) -std=c++17
LDFLAGS = -shared -B $(PYTHON)/compiler_compat -L$(PYTHON)/lib -Wl,-rpath=$(PYTHON)/lib 
LIBS = -L$(ARROW) -l:libarrow.so.1300 -l:libarrow_python.so -l:libpython$(PYTHON_VERSION).so -lsqlite3

# Targets
all: nfa.cpython-37m-x86_64-linux-gnu.so interval_nfa.cpython-37m-x86_64-linux-gpu.so  interval_vector.cpython-37m-x86_64-linux-gpu.so

nfa.o: nfa_cep.cpp
	$(CXX) $(CFLAGS) -c $< -o $@

interval_nfa.o: interval_nfa_cep.cpp
	$(CXX) $(CFLAGS) -c $< -o $@

interval_vector.o: interval_vector_cep.cpp
	$(CXX) $(CFLAGS) -c $< -o $@

nfa.cpython-37m-x86_64-linux-gnu.so: nfa.o
	$(CXX) $(LDFLAGS) $^ $(LIBS) -o $@

interval_nfa.cpython-37m-x86_64-linux-gpu.so: interval_nfa.o
	$(CXX) $(LDFLAGS) $^ $(LIBS) -o $@

interval_vector.cpython-37m-x86_64-linux-gpu.so: interval_vector.o
	$(CXX) $(LDFLAGS) $^ $(LIBS) -o $@

clean:
	rm -f *.o *.so

PHONY: all clean
