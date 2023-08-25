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
