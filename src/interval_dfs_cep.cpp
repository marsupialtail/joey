#include "common.h"
#include <deque>

typedef std::map<std::string, Scalar> fate_t;

extern "C" {
Vector2D MyFunction(PyObject ** obj_array, 
                    PyObject *obj2, 
                    KeyStringListPair *obj3,
                    KeyStringListPair *obj4,
                    const char **obj5, 
                    int num_events, 
                    const char *time_col) {

  sqlite3 *db;
  char *err_msg = nullptr;
  int rc = sqlite3_open(":memory:", &db);

  if (rc != SQLITE_OK) {
    std::cerr << "Cannot open database: " << sqlite3_errmsg(db) << std::endl;
    sqlite3_close(db);
    exit(1);
  }

  sqlite3_exec(db, "PRAGMA journal_mode = OFF;", NULL, NULL, NULL);
  sqlite3_exec(db, "PRAGMA synchronous = NORMAL;", NULL, NULL, NULL);

  if (arrow::py::import_pyarrow() != 0) {
    std::cout << "problem initializing pyarrow" << std::endl;
    throw 0;
  }

  std::map<int , std::vector<std::string>> column_names = {};
  std::map<int , std::map<std::string, size_t>> column_name_to_pos = {};
  std::map<std::string, std::shared_ptr<arrow::DataType>> types = {};
  std::map<int, size_t> row_count_indices = {};
  std::vector<std::vector<Scalar>> transposed_batch;

  for (int i = 0; i < num_events; i ++) {
    assert(arrow::py::is_table(obj_array[i]));
    arrow::Result<std::shared_ptr<arrow::Table>> result = 
        arrow::py::unwrap_table(obj_array[i]);
    assert(result.ok());
    std::shared_ptr<arrow::RecordBatch> batch = 
        result.ValueOrDie()->CombineChunksToBatch().ValueOrDie();
    
    column_names[i] = result.ValueOrDie()->ColumnNames();
    column_name_to_pos[i] = {};

    for (size_t j = 0; j < column_names[i].size(); j++) {
        column_name_to_pos[i][column_names[i][j]] = batch->schema()->GetFieldIndex(
            column_names[i][j]);
        types[column_names[i][j]] = batch->schema()->GetFieldByName(column_names[i][j])->type();
    }
    row_count_indices[i] = column_name_to_pos[i].at("__row_count__");

    
    std::vector<std::vector<Scalar>> local_batch =
      transpose_arrow_batch(batch);

    if (i == 0) {
        transposed_batch = std::move(local_batch);
    } else {
        std::string sql = "CREATE TABLE frame" + std::to_string(i) + "(";
        for (int j = 0; j < column_names[i].size(); j++) {
            sql += column_names[i][j] + " ANY, ";
        }
        sql = sql.substr(0, sql.size() - 2);
        sql += ");";
        SQLITE_EXEC_AND_CHECK(db, sql, err_msg);
        
        sql = "create index idx" + std::to_string(i) + " on frame" + std::to_string(i) + "(__row_count__);";
        SQLITE_EXEC_AND_CHECK(db, sql, err_msg);

        sqlite3_stmt * insert_stmt;
        sql = "insert into frame" + std::to_string(i) + " values(";
        for (int j = 0; j < column_names[i].size(); j++) {
            sql += "?, ";
        }
        sql = sql.substr(0, sql.size() - 2);
        sql += ");";
        
        SQLITE_PREPARE_AND_CHECK(db, sql, insert_stmt);
        // now push the entire table into the database
        for (size_t pos = 0; pos < local_batch.size(); pos++) {
            for (int j = 0; j < column_names[i].size(); j++) {
                bind_scalar_to_stmt(insert_stmt, j + 1, local_batch[pos][j]);
            }
            SQLITE_STEP_AND_CHECK(db, insert_stmt);
            SQLITE_RESET_AND_CHECK(db, insert_stmt);
        }
        
        sqlite3_finalize(insert_stmt);
    }

  }

  std::vector<std::tuple<size_t, size_t>> start_end = convert_intervals_to_start_end(obj2);
  std::map<std::string, std::vector<std::string>> event_bind_columns = processDict(obj3, num_events);
  std::map<std::string, std::vector<std::string>> event_fate_columns = processDict(obj4, num_events);
  std::vector<std::string> event_predicates = processList(obj5, num_events);
  std::vector<std::string> event_names = {};
  for (auto const &element : event_bind_columns) {
    event_names.push_back(element.first);
  }
  
  // temp tables do not improve performance

  std::vector<sqlite3_stmt *> filter_stmts(event_names.size() - 1);
  for (int event = 1; event < event_names.size(); event++) {
    // the first event predicate should be None. All following event predicates
    // should NOT be None
    std::string predicate = event_predicates[event];
    std::string sql;
    if (event == event_names.size() - 1) {
        sql = "SELECT * FROM frame" + std::to_string(event) + " WHERE " + predicate + " and __row_count__ > ? and __row_count__ < ? limit 1;";
    } else {
        sql = "SELECT * FROM frame" + std::to_string(event) + " WHERE " + predicate + " and __row_count__ > ? and __row_count__ < ?;";
    }
    std::cout << sql << std::endl;
    SQLITE_PREPARE_AND_CHECK(db, sql, filter_stmts[event - 1]);
  }


  size_t filter_calls = 0;
  size_t filter_input_total_rows = 0;
  size_t filter_output_total_rows = 0;
  std::chrono::duration<double> filter_time(0);
  std::chrono::duration<double> bind_time(0);
  std::chrono::duration<double> deque_time(0);
  std::chrono::duration<double> delete_time(0);
  std::chrono::duration<double> overhead(0);
  std::vector<std::vector<size_t>> matched_row_counts = {};
  
  auto start_time = std::chrono::high_resolution_clock::now();

  size_t start_row_count = extract_row_count_from_scalar(transposed_batch[0][row_count_indices[0]]);  

  for (std::tuple<size_t, size_t> &start_end_pair : start_end) {

    size_t start = std::get<0>(start_end_pair);
    size_t end = std::get<1>(start_end_pair) + 1;

    size_t num_rows = end - start;
    assert(num_rows > 0);

    fate_t fate = {};
    for (std::string & col: event_fate_columns[event_names[0]]) {
        fate[event_names[0] + "_" + col] = transposed_batch[start - start_row_count][column_name_to_pos[0][col]];
    }

    for (int i = 0; i < event_names.size() - 1; i++) {
        SQLITE_CLEAR_AND_CHECK(db, filter_stmts[i]);
        SQLITE_RESET_AND_CHECK(db, filter_stmts[i]);
    }

    sqlite3_stmt * filter_stmt = filter_stmts[0];
    std::string next_event_name = event_names[1];
    size_t row_count_idx = row_count_indices[1];

   
    for (int i = 0; i < event_bind_columns[next_event_name].size(); i ++)
    {
        std::string col = event_bind_columns[next_event_name].at(i);
        // print out fate
        Scalar value = fate.at(col);
        bind_scalar_to_stmt(filter_stmt, i + 1, value);
    }
    sqlite3_bind_int64(filter_stmt, event_bind_columns[next_event_name].size() + 1, (long)(start));
    sqlite3_bind_int64(filter_stmt, event_bind_columns[next_event_name].size() + 2, (long)(end));

    std::deque<size_t> stack = {};

    auto start_filter = std::chrono::high_resolution_clock::now();
    auto sqlite_result = sqlite3_step(filter_stmt);
    auto end_filter = std::chrono::high_resolution_clock::now();
    filter_time += (end_filter - start_filter);
    filter_calls ++;

    if (sqlite_result == SQLITE_ROW ) {

        std::vector<Scalar> row = {};
        size_t col_count = sqlite3_column_count(filter_stmt);
        for (int col = 0; col < col_count; col++) {
            Scalar value = recover_scalar_from_stmt(filter_stmt, col, types[column_names[1][col]]);
            row.push_back(value);
        }
        for (std::string & col: event_fate_columns[next_event_name]) {
            fate[next_event_name + "_" + col] = row[column_name_to_pos[1][col]];
        }

        stack.push_back(extract_row_count_from_scalar(row[row_count_idx]) - start);

    } else {
        SQLITE_CLEAR_AND_CHECK(db, filter_stmt);
        SQLITE_RESET_AND_CHECK(db, filter_stmt);
        continue;
    }

    while (stack.size() > 0) {
        // print out the stack

        size_t marker = stack.back();

        std::string next_event_name = event_names[stack.size() + 1];
        size_t row_count_idx = row_count_indices[stack.size() + 1];
        filter_stmt = filter_stmts[stack.size()];

        SQLITE_CLEAR_AND_CHECK(db, filter_stmt);
        for (int i = 0; i < event_bind_columns[next_event_name].size(); i ++)
        {
            std::string col = event_bind_columns[next_event_name].at(i);
            // print out fate
            Scalar value = fate.at(col);
            bind_scalar_to_stmt(filter_stmt, i + 1, value);
        }
        sqlite3_bind_int64(filter_stmt, event_bind_columns[next_event_name].size() + 1, (long)(start + marker));
        sqlite3_bind_int64(filter_stmt, event_bind_columns[next_event_name].size() + 2, (long)(end));
        
        start_filter = std::chrono::high_resolution_clock::now();
        sqlite_result = sqlite3_step(filter_stmt);
        end_filter = std::chrono::high_resolution_clock::now();
        filter_time += (end_filter - start_filter);
        filter_calls ++;

        if (sqlite_result == SQLITE_ROW ) {

            std::vector<Scalar> row = {};
            size_t col_count = sqlite3_column_count(filter_stmt);
            for (int col = 0; col < col_count; col++) {
                Scalar value = recover_scalar_from_stmt(filter_stmt, col, types[column_names[stack.size() + 1][col]]);
                row.push_back(value);
            }

            if (next_event_name == event_names[event_names.size() - 1]) {
                std::vector<size_t> result = {start};
                for (size_t element : stack) {
                    result.push_back(element + start);
                }
                result.push_back(extract_row_count_from_scalar(row[row_count_idx]));
                matched_row_counts.emplace_back(std::move(result));
                break;
                
            } else {
                for (std::string & col: event_fate_columns[next_event_name]) {
                    fate[next_event_name + "_" + col] = row[column_name_to_pos[stack.size() + 1][col]];
                }
                stack.push_back(extract_row_count_from_scalar(row[row_count_idx]) - start);
            }
            
        } else {

            SQLITE_RESET_AND_CHECK(db, filter_stmt);
            stack.pop_back();
            filter_stmt = filter_stmts[stack.size()];
            std::string this_event_name = event_names[stack.size() + 1];
            SQLITE_CLEAR_AND_CHECK(db, filter_stmt);
            for (int i = 0; i < event_bind_columns[this_event_name].size(); i ++)
            {
                std::string col = event_bind_columns[this_event_name].at(i);
                // print out fate
                Scalar value = fate.at(col);
                bind_scalar_to_stmt(filter_stmt, i + 1, value);
            }
            if (stack.size() > 0) {
                sqlite3_bind_int64(filter_stmt, event_bind_columns[this_event_name].size() + 1, (long)(start + stack.back()));
            } else {
                sqlite3_bind_int64(filter_stmt, event_bind_columns[this_event_name].size() + 1, (long)(start));
            }
                
            sqlite3_bind_int64(filter_stmt, event_bind_columns[this_event_name].size() + 2, (long)(end));

            start_filter = std::chrono::high_resolution_clock::now();
            sqlite_result = sqlite3_step(filter_stmt);
            end_filter = std::chrono::high_resolution_clock::now();
            filter_time += (end_filter - start_filter);
            filter_calls ++;

            if (sqlite_result == SQLITE_ROW ) {

                std::vector<Scalar> row = {};
                size_t col_count = sqlite3_column_count(filter_stmt);
                for (int col = 0; col < col_count; col++) {
                    Scalar value = recover_scalar_from_stmt(filter_stmt, col, types[column_names[1][col]]);
                    row.push_back(value);
                }
                for (std::string & col: event_fate_columns[this_event_name]) {
                    fate[this_event_name + "_" + col] = row[column_name_to_pos[1][col]];
                }

                stack.push_back(extract_row_count_from_scalar(row[row_count_idx]) - start);

            } else {
                continue;
            }
        
        }
        

        // auto end_overhead = std::chrono::high_resolution_clock::now();
        // overhead += std::chrono::duration_cast<std::chrono::duration<double>>(end_overhead - start_overhead);


    }

    // auto end_deque = std::chrono::high_resolution_clock::now();
    // deque_time += std::chrono::duration_cast<std::chrono::duration<double>>(end_deque - start_deque);
    
  }

  auto end_time = std::chrono::high_resolution_clock::now();
  std::chrono::duration<double> elapsed = end_time - start_time;

//   sql = "delete from frame";
//   SQLITE_EXEC_AND_CHECK(db, sql, err_msg);
  std::cout << "Number of intervals " << start_end.size() << std::endl;
  std::cout << "Filter total calls " << filter_calls << std::endl;
  std::cout << "Filter input total rows " << filter_input_total_rows << std::endl;
  std::cout << "Filter output total rows " << filter_output_total_rows << std::endl;
  std::cout << "Loop elapsed time: " << elapsed.count() << " s\n";
  std::cout << "Filter elapsed time: " << filter_time.count() << " s\n";
  std::cout << "Bind elapsed time: " << bind_time.count() << " s\n";
  std::cout << "Deque elapsed time: " << deque_time.count() << " s\n";
//   std::cout << "Delete elapsed time: " << delete_time.count() << " s\n";
  std::cout << "Overhead elapsed time: " << overhead.count() << " s\n";

  // go finalize all the prepared statements
  for (int event = 0; event < event_names.size() - 1; event++) {
    sqlite3_finalize(filter_stmts[event]);
  }

  sqlite3_close(db);

  Vector2D result;
  result.size = matched_row_counts.size();
  result.data = new Vector[result.size];

  for (size_t i = 0; i < matched_row_counts.size(); i++) {
    result.data[i].size = matched_row_counts[i].size();
    result.data[i].data = new size_t[matched_row_counts[i].size()];
    std::copy(matched_row_counts[i].begin(), matched_row_counts[i].end(),
              result.data[i].data);
  }

  return result;
}
}