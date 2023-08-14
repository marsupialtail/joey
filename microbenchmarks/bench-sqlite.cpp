#include <iostream>
#include <vector>
#include <string>
#include <ctime>
#include <sqlite3.h>

int main() {
    sqlite3* db;
    sqlite3_stmt* stmt;
    char* err_msg = nullptr;
    int rc = sqlite3_open(":memory:", &db);

    if (rc != SQLITE_OK) {
        std::cerr << "Cannot open database: " << sqlite3_errmsg(db) << std::endl;
        sqlite3_close(db);
        return 1;
    }

    std::vector<int> n_rows_choices = {1, 10, 100, 1000, 10000, 100000};
    int n_cols = 8;
    std::vector<int> n_select_choices = {1, 2, 4, 8};

    for (int n_rows : n_rows_choices) {
        for (int n_select : n_select_choices) {
            // Simulating random number generation for each column
            std::vector<std::vector<int>> rows(n_rows, std::vector<int>(n_cols));

            for (auto& row : rows) {
                for (int& val : row) {
                    val = rand() % 1000;
                }
            }

            std::string create_table_query = "CREATE TABLE test(col_0";
            for (int i = 1; i < n_cols; i++) {
                create_table_query += ", col_" + std::to_string(i);
            }
            create_table_query += ")";
            sqlite3_exec(db, create_table_query.c_str(), 0, 0, &err_msg);

            std::string insert_query = "INSERT INTO test VALUES(?";
            for (int i = 1; i < n_cols; i++) {
                insert_query += ",?";
            }
            insert_query += ")";
            rc = sqlite3_prepare_v2(db, insert_query.c_str(), -1, &stmt, NULL);

            clock_t start = clock();
            for (const auto& row : rows) {
                for (int i = 1; i <= row.size(); i ++) {
                    sqlite3_bind_int(stmt,i, row[i]);
                }
                rc = sqlite3_step(stmt);
                sqlite3_reset(stmt);
            }
            clock_t insert_time = clock() - start;

            std::string select_query = "SELECT col_0";
            for (int i = 1; i < n_select; i++) {
                select_query += ", col_" + std::to_string(i);
            }
            select_query += " FROM test WHERE col_0 > 500";
            for (int i = 1; i < n_select; i++) {
                select_query += " AND col_" + std::to_string(i) + " > 500";
            }
            rc = sqlite3_prepare_v2(db, select_query.c_str(), -1, &stmt, NULL);
            
	    size_t counter = 0;
            start = clock();
            for (int i = 0; i < 1000; i++) {
                while (sqlite3_step(stmt) == SQLITE_ROW) {
                    counter += 1;
                }
                sqlite3_reset(stmt);
            }
            clock_t select_time = clock() - start;

            std::cout << n_rows << " " << n_select << " " 
                      << static_cast<double>(select_time) / CLOCKS_PER_SEC << " results count " << counter <<  std::endl;

            sqlite3_finalize(stmt);
            sqlite3_exec(db, "DROP TABLE test", 0, 0, &err_msg);
        }
    }

    sqlite3_close(db);
    return 0;
}


