# Variables
CXX = g++
ARROW=/home/ziheng/miniconda3/envs/quokka-dev/lib/python3.8/site-packages/pyarrow
PYTHON=/home/ziheng/miniconda3/envs/quokka-dev
PYTHON_VERSION=3.8
CFLAGS = -O3 -g -fPIC -I$(ARROW)/include/ -I$(PYTHON)/include/python$(PYTHON_VERSION) -std=c++17
LDFLAGS = -shared -B $(PYTHON)/compiler_compat -L$(PYTHON)/lib -Wl,-rpath,$(PYTHON)/lib 
LIBS = -L$(ARROW) -l:libarrow.so -l:libarrow_python.so -l:libpython$(PYTHON_VERSION).so -lsqlite3
# LIBS = -L$(ARROW) -l:libarrow.so -l:libarrow_python.so -lsqlite3

# Targets
all: nfa.so interval_nfa.so  interval_vector.so interval_dfs.so

nfa.o: nfa_cep.cpp
	$(CXX) $(CFLAGS) -c $< -o $@

interval_nfa.o: interval_nfa_cep.cpp
	$(CXX) $(CFLAGS) -c $< -o $@

interval_vector.o: interval_vector_cep.cpp
	$(CXX) $(CFLAGS) -c $< -o $@

interval_dfs.o : interval_dfs_cep.cpp
	$(CXX) $(CFLAGS) -c $< -o $@

nfa.so: nfa.o
	$(CXX) $(LDFLAGS) $^ $(LIBS) -o $@

interval_nfa.so: interval_nfa.o
	$(CXX) $(LDFLAGS) $^ $(LIBS) -o $@

interval_vector.so: interval_vector.o
	$(CXX) $(LDFLAGS) $^ $(LIBS) -o $@

interval_dfs.so: interval_dfs.o
	$(CXX) $(LDFLAGS) $^ $(LIBS) -o $@

clean:
	rm -f *.o *.so

PHONY: all clean
