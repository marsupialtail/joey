#include "common.h"

extern "C"{
Vector2D  MyFunction(PyObject * obj1, PyObject * obj2, KeyStringListPair* obj3, KeyStringListPair* obj4, const char** obj5, DictEntry* obj6, int num_events, int num_indices, const char * time_col)
{

    sqlite3* db;
    sqlite3_stmt* stmt;
    char* err_msg = nullptr;
    int rc = sqlite3_open(":memory:", &db);

    if (rc != SQLITE_OK) {
        std::cerr << "Cannot open database: " << sqlite3_errmsg(db) << std::endl;
        sqlite3_close(db);
        exit(1);
    }
    sqlite3_exec(db, "PRAGMA synchronous=OFF", 0, 0, 0);
    sqlite3_exec(db, "PRAGMA journal_mode=OFF", 0, 0, 0);


	if(arrow::py::import_pyarrow() != 0){std::cout << "problem initializing pyarrow" << std::endl;throw 0;}
	assert(arrow::py::is_table(obj1));
	assert(arrow::py::is_table(obj2));
	arrow::Result<std::shared_ptr<arrow::Table>> result1 = arrow::py::unwrap_table(obj1);
	arrow::Result<std::shared_ptr<arrow::Table>> result2 = arrow::py::unwrap_table(obj2);
    assert(result1.ok());
    assert(result2.ok());
    std::vector<std::string> column_names = result1.ValueOrDie()->ColumnNames();
	std::shared_ptr<arrow::RecordBatch> batch = result1.ValueOrDie()->CombineChunksToBatch().ValueOrDie();
	std::shared_ptr<arrow::RecordBatch> intervals = result2.ValueOrDie()->CombineChunksToBatch().ValueOrDie();

    std::vector<std::tuple<size_t, size_t>> start_end = {};
    for (size_t i = 0; i < intervals->num_rows(); i++) {
        start_end.push_back(std::make_tuple(
            std::static_pointer_cast<arrow::UInt32Array>(intervals->GetColumnByName("__arc__"))->Value(i),
            std::static_pointer_cast<arrow::UInt32Array>(intervals->GetColumnByName("__crc__"))->Value(i)
        ));
    }

    std::map<std::string, std::vector<std::string>> event_required_columns = processDict(obj3, num_events);
    std::map<std::string, std::vector<std::string>> event_independent_columns = processDict(obj4, num_events);
    std::map<std::string, std::unordered_set<int>> event_indices = processDictSet(obj6, num_indices);
    std::vector<std::string> event_predicates = processList(obj5, num_events);
    // put the keys of event_required_columns into a vector
    std::vector<std::string> event_names = {};
    for (auto const& element : event_required_columns) {
        event_names.push_back(element.first);
    }

    std::vector<sqlite3_stmt*> insert_stmts (event_names.size() - 1);
    std::vector<sqlite3_stmt*> filter_stmts (event_names.size() - 1);
    std::vector<sqlite3_stmt*> delete_stmts (event_names.size() - 1);
    std::vector<std::string> current_cols = {};
    std::vector<std::shared_ptr<arrow::DataType>> types = {};
    std::vector<int> offsets = {0};

    std::shared_ptr<arrow::Schema> schema = batch->schema();
    int num_fields = schema->num_fields();

    std::map<std::string, std::vector<int>> event_required_column_pos;
    std::map<std::string, std::vector<int>> event_independent_column_pos;
    std::map<std::string, std::vector<std::shared_ptr<arrow::DataType>>> event_independent_column_types;

    for (int event = 0; event < event_names.size(); event ++) {
        std::string event_name = event_names[event];
        std::vector<int> pos = {};
        std::vector<std::shared_ptr<arrow::DataType>> this_types = {};
        std::vector<int> pos2 = {};

        for (int i = 0; i < event_independent_columns[event_name].size(); i++) {
            pos.push_back(schema->GetFieldIndex(event_independent_columns[event_name][i]));
            this_types.push_back(schema->GetFieldByName(event_independent_columns[event_name][i])->type());
        }

        for (int i = 0; i < event_required_columns[event_name].size(); i++) {
            pos2.push_back(schema->GetFieldIndex(event_required_columns[event_name][i]));
        }
        event_independent_column_pos[event_name] = pos;
        event_required_column_pos[event_name] = pos2;
        event_independent_column_types[event_name] = this_types;
    }

    for (int event = 0; event < event_names.size() - 1; event++) {
        std::string event_name = event_names[event];
        
        for (int i = 0; i < event_required_columns[event_name].size(); i++) {
            current_cols.push_back(event_name + "_" + event_required_columns[event_name][i]);
            types.push_back(schema->GetFieldByName(event_required_columns[event_name][i])->type());
        }

        offsets.push_back(offsets.back() + current_cols.size());
        std::string sql = "CREATE TABLE matched_sequences_" + std::to_string(event) + " (";
        for (int i = 0; i < current_cols.size(); i++) {
            sql += current_cols[i] + " ANY, ";
        }
        sql = sql.substr(0, sql.size() - 2);
        sql += ");";
        // std::cout << sql << std::endl;
        SQLITE_EXEC_AND_CHECK(db, sql, err_msg);

    }

    std::vector<size_t> row_count_idx = {};
    for (int i = 0; i < current_cols.size(); i++) {
        if (current_cols[i].find("__row_count__") != std::string::npos) {
            row_count_idx.push_back(i);
        }
    }
    size_t row_count_idx_in_batch = schema->GetFieldIndex("__row_count__");        

    for (int event = 0; event < event_names.size(); event++) {
        if (event < event_names.size() - 1) {
            std::string sql = "INSERT INTO matched_sequences_" + std::to_string(event) + " values (";
            for (int i = offsets[event]; i < offsets[event + 1]; i++) {
                sql += "?, ";
            }
            sql = sql.substr(0, sql.size() - 2);
            sql += ");";
            // std::cout << sql << std::endl;
            SQLITE_PREPARE_AND_CHECK(db, sql, insert_stmts[event]);
        }

        // the first event predicate should be None. All following event predicates should NOT be None
        if (event > 0){
            std::string predicate = event_predicates[event];
            std::string sql = "SELECT * FROM matched_sequences_" + std::to_string(event - 1) + " WHERE " + predicate + ";";
            // std::cout << sql << std::endl;
            SQLITE_PREPARE_AND_CHECK(db, sql, filter_stmts[event - 1]);
        }
    }

    for (int event = 0; event < event_names.size() - 1; event++) {
        std::string sql = "DELETE FROM matched_sequences_" + std::to_string(event) + ";";
        SQLITE_PREPARE_AND_CHECK(db, sql, delete_stmts[event]);
    }


    std::chrono::duration<double> filter_time(0);
    std::chrono::duration<double> bind_time(0);
    std::chrono::duration<double> overhead(0);
    std::vector<std::vector<size_t>> matched_row_counts = {};
    std::vector<std::vector<Scalar>>  transposed_batch = transpose_arrow_batch(batch);

    // precompute this_row_can_be for every row

    std::vector<std::vector<int>> rows_can_be = {};
    for (size_t row = 0; row < batch->num_rows(); row ++) {
        std::vector<int> this_row_can_be = {};
        for (int event = 1; event < event_names.size(); event++) {
            if (event_indices.find(event_names[event]) == event_indices.end() || event_indices[event_names[event]].find(row) != event_indices[event_names[event]].end()) {
                this_row_can_be.push_back(event);
            }
        }
        //reverse sort this_row_can_be
        std::sort(this_row_can_be.begin(), this_row_can_be.end(), std::greater<int>());
        rows_can_be.push_back(this_row_can_be);
    }
    

    auto start_time = std::chrono::high_resolution_clock::now();

    for (std::tuple<size_t, size_t> & start_end_pair : start_end) {

        

        size_t start = std::get<0>(start_end_pair);
        size_t end = std::get<1>(start_end_pair) + 1;

        size_t num_rows = end - start;

        std::map<int, bool> empty = {};
        for (int event = 0; event < event_names.size() - 1; event++) {
            empty[event] = true;
        }
        empty[0] = false;

        auto start_bind = std::chrono::high_resolution_clock::now();
        int j =0;

        for(int pos: event_required_column_pos[event_names[0]]) {
            bind_scalar_to_stmt(insert_stmts[0], ++j, transposed_batch[start][pos]);
        }
        SQLITE_STEP_AND_CHECK(db, insert_stmts[0]);
        SQLITE_RESET_AND_CHECK(db, insert_stmts[0]);

        auto end_bind = std::chrono::high_resolution_clock::now();
        bind_time += end_bind - start_bind;

        for (size_t row = 1; row < num_rows; row++)
        {

            auto start_overhead = std::chrono::high_resolution_clock::now();

            Scalar global_row_count_scalar = transposed_batch[row + start][row_count_idx_in_batch];
            size_t global_row_count;
            if (std::holds_alternative<int> (global_row_count_scalar)) {
                global_row_count = std::get<int>(global_row_count_scalar);
            } else if (std::holds_alternative<long> (global_row_count_scalar)) {
                global_row_count = std::get<long>(global_row_count_scalar);
            } else {
                std::cout << "error: row count type not understood" << std::endl;
                exit(1);
            }

            bool early_exit = false;

            for (int seq_len : rows_can_be[global_row_count]) {
                    
                if (empty[seq_len - 1]) {
                    continue;
                }

                auto start_bind = std::chrono::high_resolution_clock::now();
                
                // bind_row_to_sqlite(db, filter_stmts[seq_len - 1], batch, row, event_independent_columns[event_names[seq_len]]);

                for (int i = 0; i < event_independent_columns[event_names[seq_len]].size(); i++) {
                    auto pos = event_independent_column_pos[event_names[seq_len]][i];
                    auto type = event_independent_column_types[event_names[seq_len]][i];
                    Scalar item = transposed_batch[global_row_count][pos];    
                    bind_scalar_to_stmt(filter_stmts[seq_len - 1], i + 1, item);
                }

                auto end_bind = std::chrono::high_resolution_clock::now();
                bind_time += end_bind - start_bind;
                            
                auto start_filter = std::chrono::high_resolution_clock::now();

                std::vector<std::vector<Scalar>> matched = {};
                while (sqlite3_step(filter_stmts[seq_len - 1]) == SQLITE_ROW) {
                    std::vector<Scalar> row = {};
                    for (int col = 0; col < sqlite3_column_count(filter_stmts[seq_len - 1]); col++) {
                        Scalar value = recover_scalar_from_stmt(filter_stmts[seq_len - 1], col, types[col]);
                        row.push_back(value);
                    }
                    matched.push_back(row);
                }
                SQLITE_CLEAR_AND_CHECK(db, filter_stmts[seq_len - 1]);
                SQLITE_RESET_AND_CHECK(db, filter_stmts[seq_len - 1]);

                auto end_filter = std::chrono::high_resolution_clock::now();
                filter_time += end_filter - start_filter;
                
                if (matched.size() > 0) {

                    if (seq_len == event_names.size() - 1) {

                        auto matched_row = matched[0];
                        // for (std::vector<Scalar> & matched_row : matched) {
                        std::vector<size_t> row_counts = {};
                        for(int i = 0; i < row_count_idx.size(); i++) {
                            Scalar row_count = matched_row[row_count_idx[i]];
                            if (std::holds_alternative<int> (row_count)) {
                                row_counts.push_back(std::get<int>(row_count));
                            } else if (std::holds_alternative<long> (row_count)) {
                                row_counts.push_back(std::get<long>(row_count));
                            } else {
                                std::cout << "error: row count type not understood" << std::endl;
                                throw 0;
                            } 
                        }
                        row_counts.push_back(global_row_count);
                        matched_row_counts.push_back(row_counts);
                        
                        //}
                        early_exit = true;

                        break;
                    } else {
                        
                        start_bind = std::chrono::high_resolution_clock::now();
                        
                        for (std::vector<Scalar> & matched_row : matched) {

                            int j = 0;
                            for(Scalar & item : matched_row){
                                bind_scalar_to_stmt(insert_stmts[seq_len], ++j, item);
                            }

                            for(int pos: event_required_column_pos[event_names[seq_len]]) {
                                bind_scalar_to_stmt(insert_stmts[seq_len], ++j, transposed_batch[global_row_count][pos]);
                            }
                            
                            SQLITE_STEP_AND_CHECK(db, insert_stmts[seq_len]);
                            SQLITE_RESET_AND_CHECK(db, insert_stmts[seq_len]);

                        }

                        end_bind = std::chrono::high_resolution_clock::now();
                        bind_time += end_bind - start_bind;

                        empty[seq_len] = false;
                    }
                }

                auto end_overhead = std::chrono::high_resolution_clock::now();
                overhead += end_overhead - start_overhead;

                if(early_exit) break;
            }

        }

        for(int seq_len = 0; seq_len < event_names.size() - 1; seq_len++){
            if (empty[seq_len]) {
                continue;
            }
            SQLITE_STEP_AND_CHECK(db, delete_stmts[seq_len]);
            SQLITE_RESET_AND_CHECK(db, delete_stmts[seq_len]);
        }

    }

    auto end_time = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> elapsed = end_time - start_time;
    std::cout << "Loop elapsed time: " << elapsed.count() << " s\n";
    std::cout << "Filter elapsed time: " << filter_time.count() << " s\n";
    std::cout << "Bind elapsed time: " << bind_time.count() << " s\n";
    std::cout << "Overhead elapsed time: " << overhead.count() << " s\n";

    // go finalize all the prepared statements
    for (int event = 0; event < event_names.size() - 1; event++) {
        sqlite3_finalize(insert_stmts[event]);
        sqlite3_finalize(filter_stmts[event]);
        sqlite3_finalize(delete_stmts[event]);
    }

    sqlite3_close(db);

    Vector2D result;
    result.size = matched_row_counts.size();
    result.data = new Vector[result.size];

    for (size_t i = 0; i < matched_row_counts.size(); i++) {
        result.data[i].size = matched_row_counts[i].size();
        result.data[i].data = new size_t[matched_row_counts[i].size()];
        std::copy(matched_row_counts[i].begin(), matched_row_counts[i].end(), result.data[i].data);
    }

    return result;
}
}