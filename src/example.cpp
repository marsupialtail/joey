#include <arrow/python/pyarrow.h>
#include <arrow/python/platform.h>
#include "arrow/python/init.h"
#include "arrow/python/datetime.h"
#include <memory>
#include <iostream>
#include <vector>
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


template <typename BuilderType, typename T>
void AppendValues(BuilderType* builder, const std::vector<T>& values,
                  const std::vector<bool>& is_valid) {
  for (size_t i = 0; i < values.size(); ++i) {
    if (is_valid.size() == 0 || is_valid[i]) {
      builder->Append(values[i]);
    } else {
      builder->AppendNull();
    }
  }
}
//std::vector<int64_t> f0_values = {0, 1, 2, 3};
//std::vector<bool> is_valid = {true, true, true, true};
//AppendValues<arrow::Int64Builder, int64_t>(&b0, f0_values, is_valid);


PyObject *  MyFunction(PyObject * obj1, PyObject * obj2, int tolerance)

{

	if(arrow::py::import_pyarrow() != 0)
         {
		std::cout << "problem initializing pyarrow" << std::endl;
		throw 0;}
        //std::cout << "test" << std::endl;
	assert(arrow::py::is_batch(obj1));
	assert(arrow::py::is_batch(obj2));
	arrow::Result<std::shared_ptr<arrow::RecordBatch>> result1 = arrow::py::unwrap_batch(obj1);
	arrow::Result<std::shared_ptr<arrow::RecordBatch>> result2 = arrow::py::unwrap_batch(obj2);
        assert(result1.ok());
        assert(result2.ok());
	std::shared_ptr<arrow::RecordBatch> batch1, batch2;
	auto trade = result1.ValueOrDie();
	auto quote = result2.ValueOrDie();
	auto trade_schema = trade->schema();
	auto quote_schema = quote->schema();
	
        std::shared_ptr<arrow::Array> trade_ts = trade->column(0);
        std::shared_ptr<arrow::Array> quote_ts = quote->column(0);
	int trade_length = trade_ts->length();
	int quote_length = quote_ts->length();

	std::cout << "trade length " << trade_length << std::endl;
	std::cout << "quote length " << quote_length << std::endl;

	std::shared_ptr<arrow::Int64Array> int_array1 = std::dynamic_pointer_cast<arrow::Int64Array>(trade_ts);
	if(! int_array1){throw 0;}
 	const int64_t* trade_ts_raw = int_array1->raw_values();
	std::shared_ptr<arrow::Int64Array> int_array2 = std::dynamic_pointer_cast<arrow::Int64Array>(quote_ts);
	if(! int_array2){throw 0;}
 	const int64_t* quote_ts_raw = int_array2->raw_values();

	int quote_finger =0 , trade_finger = 0;

	arrow::Int64Builder joined_trade_ts;
	arrow::Int64Builder joined_quote_ts;
	arrow::Int64Builder joined_trade_idx;
	arrow::Int64Builder joined_quote_idx;
	int result_length = 0;
	while (quote_finger < quote_length && trade_finger < trade_length){
		auto trade_val = trade_ts_raw[trade_finger];
		auto quote_val = quote_ts_raw[quote_finger];
		if(quote_val < trade_val - tolerance)
		{
			quote_finger += 1;
			continue;
		}
		if(quote_val > trade_val)
		{
			trade_finger += 1;
			continue;
		}
		for (int pos = quote_finger; pos < quote_length; pos ++)
		{
			auto this_quote = quote_ts_raw[pos];
			if(this_quote <= trade_val)
			{
				joined_quote_idx.Append(pos);
				joined_trade_idx.Append(trade_finger);
				joined_quote_ts.Append(this_quote);
				joined_trade_ts.Append(trade_val);
				result_length ++;
			} else
			{
				break;
			}
		}
		trade_finger += 1;
	}
        

	std::shared_ptr<arrow::Array> a0, a1, a2, a3;
	joined_trade_ts.Finish(&a0);
	joined_trade_idx.Finish(&a2);
	joined_quote_ts.Finish(&a1);
	joined_quote_idx.Finish(&a3);
	auto f0 = arrow::field("trade_ts", arrow::int64());
	auto f1 = arrow::field("quote_ts", arrow::int64());
	auto f2 = arrow::field("trade_idx", arrow::int64());
	auto f3 = arrow::field("quote_idx", arrow::int64());
	auto result_schema = arrow::schema({f0, f1, f2, f3});
	auto expected = arrow::RecordBatch::Make(result_schema, result_length, {a0, a1,a2, a3});

	return arrow::py::wrap_batch(expected);	


}

