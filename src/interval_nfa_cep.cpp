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

	if(arrow::py::import_pyarrow() != 0){std::cout << "problem initializing pyarrow" << std::endl;throw 0;}
	assert(arrow::py::is_table(obj1));
	assert(arrow::py::is_table(obj2));
	arrow::Result<std::shared_ptr<arrow::Table>> result1 = arrow::py::unwrap_table(obj1);
	arrow::Result<std::shared_ptr<arrow::Table>> result2 = arrow::py::unwrap_table(obj2);
    assert(result1.ok());
    assert(result2.ok());
	std::shared_ptr<arrow::RecordBatch> batch = result1.ValueOrDie()->CombineChunksToBatch().ValueOrDie();
	std::shared_ptr<arrow::RecordBatch> intervals = result2.ValueOrDie()->CombineChunksToBatch().ValueOrDie();

    std::map<std::string, std::vector<std::string>> event_required_columns = processDict(obj3, num_events);
    std::map<std::string, std::vector<std::string>> event_independent_columns = processDict(obj4, num_events);
    std::map<std::string, std::set<int>> event_indices = processDictSet(obj6, num_indices);
    std::vector<std::string> event_predicates = processList(obj5, num_events);
    // put the keys of event_required_columns into a vector
    std::vector<std::string> event_names = {};
    for (auto const& element : event_required_columns) {
        event_names.push_back(element.first);
    }

    std::vector<sqlite3_stmt*> insert_stmts (event_names.size() - 1);
    std::vector<sqlite3_stmt*> filter_stmts (event_names.size() - 1);
    std::vector<std::string> current_cols = {};
    std::vector<int> offsets = {0};
    for (int event = 0; event < event_names.size() - 1; event++) {
        std::string event_name = event_names[event];
        for (int i = 0; i < event_required_columns[event_name].size(); i++) {
            current_cols.push_back(event_name + "_" + event_required_columns[event_name][i]);
        }
        offsets.push_back(offsets.back() + current_cols.size());
        std::string sql = "CREATE TABLE matched_sequences_" + std::to_string(event) + " (";
        for (int i = 0; i < current_cols.size(); i++) {
            sql += current_cols[i] + " NUMERIC, ";
        }
        sql = sql.substr(0, sql.size() - 2);
        sql += ");";
        std::cout << sql << std::endl;
        SQLITE_EXEC_AND_CHECK(db, sql, err_msg);

        // cur = cur.execute("CREATE INDEX idx_{} ON matched_sequences_{}({});".format(event, event, event_names[0] + "_" + time_col))
    
        sql = "CREATE INDEX idx_" + std::to_string(event) + " ON matched_sequences_" + std::to_string(event) + "(" + event_names[0] + "_" + time_col +  ");";
        std::cout << sql << std::endl;
        SQLITE_EXEC_AND_CHECK(db, sql, err_msg);

    }

    std::vector<size_t> row_count_idx = {};
    for (int i = 0; i < current_cols.size(); i++) {
        if (current_cols[i].find("__row_count__") != std::string::npos) {
            row_count_idx.push_back(i);
        }
    }        

    for (int event = 0; event < event_names.size(); event++) {
        if (event < event_names.size() - 1) {
            std::string sql = "INSERT INTO matched_sequences_" + std::to_string(event) + " values (";
            for (int i = offsets[event]; i < offsets[event + 1]; i++) {
                sql += "?, ";
            }
            sql = sql.substr(0, sql.size() - 2);
            sql += ");";
            std::cout << sql << std::endl;
            SQLITE_PREPARE_AND_CHECK(db, sql, insert_stmts[event]);
        }

        // the first event predicate should be None. All following event predicates should NOT be None
        if (event > 0){
            std::string predicate = event_predicates[event];
            std::string sql = "SELECT * FROM matched_sequences_" + std::to_string(event - 1) + " WHERE " + predicate + ";";
            std::cout << sql << std::endl;
            SQLITE_PREPARE_AND_CHECK(db, sql, filter_stmts[event - 1]);
        }
    }


    std::map<int, bool> empty = {};
    for (int event = 0; event < event_names.size() - 1; event++) {
        empty[event] = true;
    }
    

    std::string sql = "SELECT * FROM matched_sequences_0 LIMIT 1;";
    rc = sqlite3_prepare_v2(db, sql.c_str(), -1, &stmt, 0);
    if (rc != SQLITE_OK) {
        std::cerr << "Cannot prepare statement: " << sqlite3_errmsg(db) << std::endl;
        sqlite3_close(db);
        exit(1);
    }

    // Execute the SQL statement and print results
    while (sqlite3_step(stmt) == SQLITE_ROW) {
        for (int col = 0; col < sqlite3_column_count(stmt); col++) {
            std::cout << sqlite3_column_text(stmt, col) << " ";
        }
        std::cout << std::endl;
    }

    size_t num_rows = batch->num_rows();
    size_t total_matched = 0;
    std::vector<std::vector<size_t>> matched_row_counts = {};

    for (size_t row = 0; row < num_rows; row++)
    {

        double progress = (double) row / num_rows;
        display_progress(progress);
        auto global_row_count = std::static_pointer_cast<arrow::UInt32Array>(batch->GetColumnByName("__row_count__"))->Value(row);
        // std::cout << global_row_count << std::endl;
        // this_row_can_be = [i for i in range(1, total_events) if event_indices[event_names[i]] is None or global_row_count in event_indices[event_names[i]]]

        std::vector<int> this_row_can_be = {};
        for (int event = 1; event < event_names.size(); event++) {
            if (event_indices.find(event_names[event]) == event_indices.end() || event_indices[event_names[event]].find(global_row_count) != event_indices[event_names[event]].end()) {
                this_row_can_be.push_back(event);
            }
        }
        
        bool early_exit = false;

        for (int seq_len : this_row_can_be) {
                
            if (empty[seq_len - 1]) {
                continue;
            }
            
            std::string sql = "DELETE FROM matched_sequences_" + std::to_string(seq_len - 1) 
                + " WHERE " + event_names[0] + "_" + time_col + " < " 
                + std::to_string(std::static_pointer_cast<arrow::UInt64Array>(
                    batch->GetColumnByName(time_col))->Value(row) - 7200);

            SQLITE_EXEC_AND_CHECK(db, sql, err_msg);

            bind_row_to_sqlite(db, filter_stmts[seq_len - 1], batch, row, event_independent_columns[event_names[seq_len]]);
            
            std::vector<std::vector<std::string>> matched = {};
            while (sqlite3_step(filter_stmts[seq_len - 1]) == SQLITE_ROW) {
                std::vector<std::string> row = {};
                for (int col = 0; col < sqlite3_column_count(filter_stmts[seq_len - 1]); col++) {
                    row.push_back((const char*)sqlite3_column_text(filter_stmts[seq_len - 1], col));
                }
                matched.push_back(row);
            }
            SQLITE_CLEAR_AND_CHECK(db, filter_stmts[seq_len - 1]);
            SQLITE_RESET_AND_CHECK(db, filter_stmts[seq_len - 1]);

            if (matched.size() > 0) {
                if (seq_len == event_names.size() - 1) {
                    
                    std::vector<size_t> row_counts = {};
                    for(int i = 0; i < row_count_idx.size(); i++) {
                        row_counts.push_back(std::stoul(matched[0][row_count_idx[i]]));
                    }
                    row_counts.push_back(global_row_count);
                    matched_row_counts.push_back(row_counts);
                    early_exit = true;
                    break;
                } else {
                    for (std::vector<std::string> & matched_row : matched) {
                        int j = 0;
                        for(std::string & item : matched_row){
                            sqlite3_bind_text(insert_stmts[seq_len], j + 1, item.c_str(), -1, SQLITE_TRANSIENT);
                            j += 1;
                        }
                        bind_row_to_sqlite(db, insert_stmts[seq_len], batch, row, event_required_columns[event_names[seq_len]], matched_row.size() - 1);
                        SQLITE_STEP_AND_CHECK(db, insert_stmts[seq_len]);
                        SQLITE_RESET_AND_CHECK(db, insert_stmts[seq_len]);
                    }
                    
                    empty[seq_len] = false;
                }
            }

            if(early_exit) break;
        }

        if (event_indices.find(event_names[0]) == event_indices.end() || event_indices[event_names[0]].find(global_row_count) != event_indices[event_names[0]].end()) {
            bind_row_to_sqlite(db, insert_stmts[0], batch, row, event_required_columns[event_names[0]]);
            SQLITE_STEP_AND_CHECK(db, insert_stmts[0]);
            SQLITE_RESET_AND_CHECK(db, insert_stmts[0]);
            empty[0] = false;
        }

    }

    // go finalize all the prepared statements
    for (int event = 0; event < event_names.size() - 1; event++) {
        sqlite3_finalize(insert_stmts[event]);
        sqlite3_finalize(filter_stmts[event]);
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