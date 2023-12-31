#include <arrow/python/pyarrow.h>
#include <arrow/python/platform.h>
#include "arrow/python/init.h"
#include "arrow/python/datetime.h"
#include <memory>
#include <iostream>
#include <vector>
#include<tuple>
#include <map>
#include <unordered_set>
//#include <gtest/gtest.h>

#include "arrow/array/array_base.h"
#include "arrow/array/builder_binary.h"
#include "arrow/array/builder_dict.h"
#include "arrow/array/builder_nested.h"
#include "arrow/array/builder_primitive.h"
#include "arrow/record_batch.h"
#include "arrow/status.h"
#include "arrow/table_builder.h"
//#include "arrow/testing/gtest_util.h"
#include "arrow/type.h"
#include "arrow/type_fwd.h"
#include "arrow/util/checked_cast.h"
#include <sqlite3.h>
#include <arrow/table.h>
#include <cctype>  // for std::toupper
#include <chrono>

#include <variant>

typedef std::variant<int, long, double, std::string> Scalar;

std::string to_upper(const std::string& input) {
    std::string result = input;
    std::transform(result.begin(), result.end(), result.begin(),
        [](unsigned char c) { return std::toupper(c); });
    return result;
}

#define SQLITE_PREPARE_AND_CHECK(db, sql, stmt)               \
    do {                                                      \
        int rc = sqlite3_prepare_v2(db, sql.c_str(), -1, &stmt, 0); \
        if (rc != SQLITE_OK) {                               \
            std::cerr << "Cannot prepare statement: " << sql.c_str() << " " \
                      << sqlite3_errmsg(db) << std::endl;    \
            sqlite3_close(db);                               \
            exit(1);                                         \
        }                                                    \
    } while(0)

#define SQLITE_EXEC_AND_CHECK(db, sql, err_msg)                  \
    do {                                                        \
        int rc = sqlite3_exec(db, sql.c_str(), 0, 0, &err_msg); \
        if (rc != SQLITE_OK) {                                  \
            std::cerr << "SQL error: " << sql.c_str() << " "    \
                      << err_msg << std::endl;                  \
            sqlite3_free(err_msg);                              \
            sqlite3_close(db);                                  \
            exit(1);                                            \
        }                                                       \
    } while (0)

#define SQLITE_STEP_AND_CHECK(db, stmt)                   \
    do {                                                       \
        int rc = sqlite3_step(stmt);                          \
        if (rc != SQLITE_OK && rc != SQLITE_DONE) {                                 \
            std::cerr << "cannot step statement: "            \
                      << sqlite3_errmsg(db) << std::endl;      \
            sqlite3_close(db);                                 \
            exit(1);                                           \
        }                                                      \
    } while (0)

#define SQLITE_RESET_AND_CHECK(db, stmt)                   \
    do {                                                       \
        int rc = sqlite3_reset(stmt);                          \
        if (rc != SQLITE_OK) {                                 \
            std::cerr << "cannot reset statement: "            \
                      << sqlite3_errmsg(db) << std::endl;      \
            sqlite3_close(db);                                 \
            exit(1);                                           \
        }                                                      \
    } while (0)

#define SQLITE_CLEAR_AND_CHECK(db, stmt)                   \
    do {                                                       \
        int rc = sqlite3_clear_bindings(stmt);                          \
        if (rc != SQLITE_OK) {                                 \
            std::cerr << "cannot clear bindings statement: "            \
                      << sqlite3_errmsg(db) << std::endl;      \
            sqlite3_close(db);                                 \
            exit(1);                                           \
        }                                                      \
    } while (0)




struct StringList {
    const char** items;
    int length;
};

struct KeyStringListPair {
    const char* key;
    StringList values;
};

struct IntSet {
    int* data;
    int length;
};

struct DictEntry {
    const char* key;
    IntSet values;
};

typedef struct {
    size_t* data;
    size_t size;
} Vector;

typedef struct {
    Vector* data;
    size_t size;
} Vector2D;

std::map<std::string, std::vector<std::string>> processDict(const KeyStringListPair* obj3, int num_keys) {
    std::map<std::string, std::vector<std::string>> result  = {};
    std::cout << "length" << num_keys << std::endl;
    for(int i = 0; i < num_keys; i++) {
        std::vector<std::string> value_list;
        for(int j = 0; j < obj3[i].values.length; j++) {
            value_list.push_back(obj3[i].values.items[j]);
        }
        
        result[obj3[i].key] = value_list;
    }
    return result;
}

std::vector<std::string> processList(const char** pairs, int num_keys) {
    std::vector<std::string> list;
    for (int i = 0; i < num_keys; i++) {
        list.push_back(pairs[i]);
        std::cout << pairs[i] << std::endl;
    }    
    return list;
}

std::map<std::string, std::unordered_set<int>> processDictSet(DictEntry* entries, int length) {
    std::map<std::string, std::unordered_set<int>> cpp_map;
    for (int i = 0; i < length; i++) {
        std::unordered_set<int> s(entries[i].values.data, entries[i].values.data + entries[i].values.length);
        cpp_map[entries[i].key] = s;
    }
    return cpp_map;
}

void display_progress(double progress) {
    int barWidth = 70;

    std::cout << "[";
    int pos = barWidth * progress;
    for (int i = 0; i < barWidth; ++i) {
        if (i < pos) std::cout << "=";
        else if (i == pos) std::cout << ">";
        else std::cout << " ";
    }
    std::cout << "] " << int(progress * 100.0) << " %\r";
    std::cout.flush();
}

void bind_scalar_to_stmt(sqlite3_stmt* stmt, int j, Scalar item){
    if (std::holds_alternative<int> (item)) {
        sqlite3_bind_int(stmt, j , std::get<int>(item));
    } else if (std::holds_alternative<long> (item)) {
        sqlite3_bind_int64(stmt, j , std::get<long>(item));
    } else if (std::holds_alternative<double>(item)) {
        sqlite3_bind_double(stmt, j , std::get<double>(item));
    } else if (std::holds_alternative<std::string>(item)) {
        sqlite3_bind_text(stmt, j , std::get<std::string>(item).c_str(), -1, SQLITE_TRANSIENT);
    } else {
        std::cout << "Unsupported type " << item.index() << std::endl;
        exit(1);
    }
}

void print_scalar(Scalar item) {
    if (std::holds_alternative<int> (item)) {
        std::cout << std::get<int>(item);
    } else if (std::holds_alternative<long> (item)) {
        std::cout << std::get<long>(item);
    } else if (std::holds_alternative<double>(item)) {
        std::cout << std::get<double>(item);
    } else if (std::holds_alternative<std::string>(item)) {
        std::cout << std::get<std::string>(item);
    } else {
        std::cout << "Unsupported type " << item.index() << std::endl;
        exit(1);
    }
}

Scalar recover_scalar_from_stmt(sqlite3_stmt* stmt, int col, std::shared_ptr<arrow::DataType> type) {
    Scalar value;
    switch(type->id()) {
        case arrow::Type::BOOL: {
            value = sqlite3_column_int(stmt, col);
            break;
        }
        case arrow::Type::DOUBLE: {
            value = sqlite3_column_double(stmt, col);
            break;
        }
        case arrow::Type::FLOAT: {
            value = sqlite3_column_double(stmt, col);
            break;
        }
        case arrow::Type::INT32: {
            value = sqlite3_column_int(stmt, col);
            break;
        }
        case arrow::Type::UINT32: {
            value = sqlite3_column_int(stmt, col);
            break;
        }
        case arrow::Type::INT64: {
            value = sqlite3_column_int64(stmt, col);
            break;
        }
        case arrow::Type::UINT64: {
            value = sqlite3_column_int64(stmt, col);
            break;
        }
        case arrow::Type::STRING: {
            value = (const char*)sqlite3_column_text(stmt, col);
            break;
        }
        default: {
            std::cout << "unsupported type" << std::endl;
            exit(1);
        }
    }
    return value;
}

std::vector<std::vector<Scalar>> transpose_arrow_batch(std::shared_ptr<arrow::RecordBatch> batch) {

    auto num_rows = batch->num_rows();
    auto num_columns = batch->num_columns();

    std::vector<std::vector<Scalar>> result(num_rows, std::vector<Scalar>(num_columns));
    
    for (int row = 0; row < num_rows; row++) {
        for (int j = 0; j < num_columns; j++) {

            int i = row;
            std::shared_ptr<arrow::Array> array1 = batch->column(j);
            std::shared_ptr<arrow::DataType> type = array1->type();

            switch(type->id()) {
                case arrow::Type::BOOL: {
                    std::shared_ptr<arrow::BooleanArray> array = std::static_pointer_cast<arrow::BooleanArray>(array1);
                    if (array->IsValid(i)) {
                        result[row][j] = (array->Value(i) ? static_cast<int>(1) : static_cast<int>(0));
                    } else {
                        std::cout << "does not support null values" << std::endl;
                        exit(1);
                    }
                    break;
                }

                case arrow::Type::DOUBLE: {
                    std::shared_ptr<arrow::DoubleArray> array = std::static_pointer_cast<arrow::DoubleArray>(array1);
                    if (array->IsValid(i)) {
                        result[row][j] = array->Value(i);
                    } else {
                        std::cout << "does not support null values" << std::endl;
                        exit(1);
                    }
                    break;
                }

                case arrow::Type::FLOAT: {
                    std::shared_ptr<arrow::FloatArray> array = std::static_pointer_cast<arrow::FloatArray>(array1);
                    if (array->IsValid(i)) {
                        result[row][j] = static_cast<double>(array->Value(i));
                    } else {
                        std::cout << "does not support null values" << std::endl;
                        exit(1);
                    }
                    break;
                }

                case arrow::Type::INT64: {
                    std::shared_ptr<arrow::Int64Array> array = std::static_pointer_cast<arrow::Int64Array>(array1);
                    if (array->IsValid(i)) {
                        result[row][j] = array->Value(i);
                    } else {
                        std::cout << "does not support null values" << std::endl;
                        exit(1);
                    }
                    break;
                }

                case arrow::Type::INT32: {
                    std::shared_ptr<arrow::Int32Array> array = std::static_pointer_cast<arrow::Int32Array>(array1);
                    if (array->IsValid(i)) {
                        result[row][j] = array->Value(i);
                    } else {
                        std::cout << "does not support null values" << std::endl;
                        exit(1);
                    }
                    break;
                }

                case arrow::Type::UINT64: {
                    std::shared_ptr<arrow::UInt64Array> array = std::static_pointer_cast<arrow::UInt64Array>(array1);
                    if (array->IsValid(i)) {
                        assert(array->Value(i) <= std::numeric_limits<int64_t>::max());
                        result[row][j] = static_cast<int64_t>(array->Value(i));
                    } else {
                        std::cout << "does not support null values" << std::endl;
                        exit(1);
                    }
                    break;
                }

                case arrow::Type::UINT32: {
                    std::shared_ptr<arrow::UInt32Array> array = std::static_pointer_cast<arrow::UInt32Array>(array1);
                    if (array->IsValid(i)) {
                        assert(array->Value(i) <= std::numeric_limits<int32_t>::max());
                        result[row][j] = static_cast<int32_t>(array->Value(i));
                    } else {
                        std::cout << "does not support null values" << std::endl;
                        exit(1);
                    }
                    break;
                }

                case arrow::Type::STRING: {
                    std::shared_ptr<arrow::StringArray> array = std::static_pointer_cast<arrow::StringArray>(array1);

                    if (array->IsValid(i)) {
                        result[row][j] = array->GetString(i);
                    } else {
                        std::cout << "does not support null values" << std::endl;
                        exit(1);
                    }
                    break;
                }


                case arrow::Type::LARGE_STRING: {
                    std::shared_ptr<arrow::LargeStringArray> array = std::static_pointer_cast<arrow::LargeStringArray>(array1);

                    if (array->IsValid(i)) {
                        result[row][j] = array->GetString(i);
                    } else {
                        std::cout << "does not support null values" << std::endl;
                        exit(1);
                    }
                    break;
                }

                default: {
                    std::cout << "Unsupported type " << type->ToString() << std::endl;
                    exit(1);
                }
                
            }
        }
    }

    return result;
    
}

void bind_row_to_sqlite(sqlite3* db, sqlite3_stmt* stmt, std::shared_ptr<arrow::RecordBatch> batch, int row, std::vector<std::string> column_names, int offset = -1) {
    int rc;

    // std::cout << schema->ToString() << std::endl;
    
    int i = row;
    int j = offset;
    for (std::string & column_name : column_names) {

        j += 1;
        std::shared_ptr<arrow::Array> array1 = batch->GetColumnByName(column_name);
        std::shared_ptr<arrow::DataType> type = array1->type();

        switch(type->id()) {
            case arrow::Type::BOOL: {
                std::shared_ptr<arrow::BooleanArray> array = std::static_pointer_cast<arrow::BooleanArray>(array1);
                if (array->IsValid(i)) {
                    rc = sqlite3_bind_int(stmt, j + 1, array->Value(i));
                } else {
                    rc = sqlite3_bind_null(stmt, j + 1);
                }
                break;
            }

            case arrow::Type::DOUBLE: {
                std::shared_ptr<arrow::DoubleArray> array = std::static_pointer_cast<arrow::DoubleArray>(array1);
                if (array->IsValid(i)) {
                    rc = sqlite3_bind_double(stmt, j + 1, array->Value(i));
                } else {
                    rc = sqlite3_bind_null(stmt, j + 1);
                }
                break;
            }

            case arrow::Type::FLOAT: {
                std::shared_ptr<arrow::FloatArray> array = std::static_pointer_cast<arrow::FloatArray>(array1);
                rc = sqlite3_bind_double(stmt, j + 1, (float)array->Value(i));
                if (array->IsValid(i)) {
                    rc = sqlite3_bind_double(stmt, j + 1, (float)array->Value(i));

                } else {
                    rc = sqlite3_bind_null(stmt, j + 1);
                }
                break;
            }

            case arrow::Type::INT64: {
                std::shared_ptr<arrow::Int64Array> array = std::static_pointer_cast<arrow::Int64Array>(array1);
                if (array->IsValid(i)) {
                    rc = sqlite3_bind_int64(stmt, j + 1, array->Value(i));
                } else {
                    rc = sqlite3_bind_null(stmt, j + 1);
                }
                break;
            }

            case arrow::Type::INT32: {
                std::shared_ptr<arrow::Int32Array> array = std::static_pointer_cast<arrow::Int32Array>(array1);
                if (array->IsValid(i)) {
                    rc = sqlite3_bind_int(stmt, j + 1, array->Value(i));
                } else {
                    rc = sqlite3_bind_null(stmt, j + 1);
                }
                break;
            }

            case arrow::Type::UINT64: {
                std::shared_ptr<arrow::UInt64Array> array = std::static_pointer_cast<arrow::UInt64Array>(array1);
                if (array->IsValid(i)) {
                    rc = sqlite3_bind_int64(stmt, j + 1, array->Value(i));
                } else {
                    rc = sqlite3_bind_null(stmt, j + 1);
                }
                break;
            }

            case arrow::Type::UINT32: {
                std::shared_ptr<arrow::UInt32Array> array = std::static_pointer_cast<arrow::UInt32Array>(array1);
                if (array->IsValid(i)) {
                    rc = sqlite3_bind_int(stmt, j + 1, array->Value(i));
                } else {
                    rc = sqlite3_bind_null(stmt, j + 1);
                }
                break;
            }

            case arrow::Type::STRING: {
                std::shared_ptr<arrow::StringArray> array = std::static_pointer_cast<arrow::StringArray>(array1);

                if (array->IsValid(i)) {
                    rc = sqlite3_bind_text(stmt, j + 1, array->GetString(i).c_str(), -1, SQLITE_TRANSIENT);
                } else {
                    rc = sqlite3_bind_null(stmt, j + 1);
                }
                break;
            }

            case arrow::Type::LARGE_STRING: {
                std::shared_ptr<arrow::LargeStringArray> array = std::static_pointer_cast<arrow::LargeStringArray>(array1);

                if (array->IsValid(i)) {
                    rc = sqlite3_bind_text(stmt, j + 1, array->GetString(i).c_str(), -1, SQLITE_TRANSIENT);
                } else {
                    rc = sqlite3_bind_null(stmt, j + 1);
                }
                break;
            }

            default: {
                rc = sqlite3_bind_null(stmt, j + 1);
                break;
            }
        }
    }
    
}