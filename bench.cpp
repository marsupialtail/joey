#include <iostream>
#include <vector>
#include <functional>
#include <chrono>

#define REPEATS 1000
#define T 800
#define T2 800
#define T3 800
#define T4 800

int main(int argc, char** argv) {
    
    std::vector<int> n_rows = {1, 5,10,25,100,1000,10000};
    int n_cols = 30;
    std::vector<std::function<bool(int *)>> functions = {};

    functions.push_back(
        [](int* x) { return (x[0] > T); }
    );

    functions.push_back(
        [](int* x) { return ((x[0] > T2) & (x[1] > T2)); }
    );

    functions.push_back(
        [](int* x) { return ((x[0] > T3) & (x[1] > T3) & (x[2] > T3) & (x[3] > T3)); }
    );

    functions.push_back(
        [](int* x) { return ((x[0] > T4) & (x[1] > T4) & (x[2] > T4) & (x[3] > T4) & (x[4] > T4) & (x[5] > T4) & (x[6] > T4) & (x[7] > T4)); }
    );

    // functions.push_back(
    //     [](int* x) { return ((x[0] > x[1])); }
    // );

    // functions.push_back(
    //     [](int* x) { return ((x[0] > 700) & (1 > (x[3] - x[1]) / (x[2] - x[0]))); }
    // );

    // functions.push_back(
    //     [](int * x) {return ((x[7] > 700) & (x[3] < x[2] * 2) & (x[6] > (x[5] - x[4]) * (x[3] -x [1]) * (x[2] - x[0]) + x[7])); }
    // );
   

    std::vector<int> v(10 * n_rows.back() * n_cols, 0);

    
    for(int i = 0; i < 10 * n_rows.back() * n_cols; i++) {
        v[i] = rand() % 1000;
    }


    
    for (int n_row : n_rows) {
                
        int counter = 1;


        for (auto filter : functions) {
    		std::vector<int> my_rows = {};
    		for(int i = 0; i < REPEATS; i++){
    		    int my_row = rand() % (10 * n_rows.back() - n_rows.back()); // do not always do the same portion to simulate cold cache or less tha ndesirable caching
    		    my_rows.push_back(my_row);
    		}
            auto start_time = std::chrono::high_resolution_clock::now();

            std::vector<int> results = {};
            for(int i = 0; i < REPEATS; i++){
		results.clear();
           	int my_row = my_rows[i];
                for(int row = my_row; row <my_row + n_row; row ++) {
                    int * col = v.data() + (row * n_cols);
                    if (filter(col)) {
                        results.insert(results.end(), v.begin() + row * n_cols, v.begin() + (row + 1) * n_cols);
                    }
                }
            }
            
            auto end_time = std::chrono::high_resolution_clock::now();
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time).count();
            std::cout << "(" << n_row << "," << counter ++ << "," << results.size() << "," <<  duration << ")," << std::endl;
        }
    }
}
