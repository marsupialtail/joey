./bench-sqlite > bench-sqlite-cpp.out
cat bench-sqlite-cpp.out | python plot_micro_bench.py --output bench-sqlite-cpp.png 

python bench-sqlite.py > bench-sqlite-python.out
cat bench-sqlite-python.out | python plot_micro_bench.py --output bench-sqlite-python.png