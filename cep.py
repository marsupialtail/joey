from nfa_cep import nfa_cep
from interval_nfa_cep import nfa_interval_cep_1
from interval_vector_cep import vector_interval_cep
import sqlglot
import polars

def verify(data, results, conditions):
    # the results should have event_name__row_count__ column

    all_predicate = ""
    events = []
    for condition in conditions:
        event_name, predicate = condition
        all_predicate += "({}) and ".format(predicate)
        event_nrs = results[event_name + "___row_count__"].to_list()
        events.append(data[event_nrs].rename({col : event_name + "_" + col for col in data[event_nrs].columns}))
    
    events = polars.concat(events, how = "horizontal")

    all_predicate = all_predicate[:-5]
    all_predicate = sqlglot.parse_one(all_predicate).transform(lambda node: sqlglot.exp.column(node.table + "_" + node.name) if isinstance(node, sqlglot.exp.Column) else node).sql()
    print(all_predicate)

    assert polars.SQLContext(frame = events).execute("select count(*) from frame where {}".format(all_predicate)).collect()["count"][0] == len(results)


# crimes = polars.read_parquet("crimes.parquet")
# results = nfa_cep(crimes, [('a', "a.primary_category_id = 27"), 
#      ('b', """b.primary_category_id = 1 and b.LATITUDE - a.LATITUDE >= -0.025
#     and b.LATITUDE - a.LATITUDE <= 0.025
#     and b.LONGITUDE - a.LONGITUDE >= -0.025
#     and b.LONGITUDE - a.LONGITUDE <= 0.025"""),
#     ('c', """c.primary_category_id = 24 and c.LATITUDE - a.LATITUDE >= -0.025
#     and c.LATITUDE - a.LATITUDE <= 0.025
#     and c.LONGITUDE - a.LONGITUDE >= -0.025
#     and c.LONGITUDE - a.LONGITUDE <= 0.025""")], "TIMESTAMP_LONG",  20000000)

# print(results)
# results.write_parquet("results.parquet")


qqq = polars.read_parquet("2021.parquet").with_row_count("row_count").with_columns(polars.col("row_count").cast(polars.Int64()))
original_qqq = qqq.with_columns(((polars.col("date").str.strptime(polars.Date).dt.timestamp('ms') // 1000) + polars.col("candle") * 60).cast(polars.UInt64()).alias("timestamp"))

qqq = original_qqq.groupby_rolling("row_count", period = "10i", offset = "-5i").agg([
        polars.col("close").min().alias("min_close"),
        polars.col("close").max().alias("max_close"),
    ])\
    .hstack(original_qqq.select(["timestamp", "close"]))\
    .with_columns([(polars.col("close") == polars.col("min_close")).alias("is_local_bottom"),
                  (polars.col("close") == polars.col("max_close")).alias("is_local_top")])

daily_qqq = polars.read_parquet("daily.parquet")
filtered_symbols = daily_qqq.groupby("symbol").agg([polars.count(), polars.sum("volume")]).filter(polars.col("count") == 252).filter(polars.col("volume") > 1e8).select(["symbol"])
daily_qqq = filtered_symbols.join(daily_qqq, "symbol")
daily_qqq = daily_qqq.sort(["symbol", "date"])
daily_qqq = polars.concat([i.with_row_count("row_count") for i in daily_qqq.partition_by("symbol")]).with_columns(polars.col("row_count").cast(polars.Int64()))
daily_qqq = daily_qqq.groupby_rolling("row_count", period = "20i", offset = "-10i", by = "symbol", check_sorted = False).agg([
        polars.col("close").min().alias("min_close"),
        polars.col("close").max().alias("max_close"),
    ])\
    .hstack(daily_qqq.select(["close"]))\
    .with_columns([(polars.col("close") == polars.col("min_close")).alias("is_local_bottom"),
                  (polars.col("close") == polars.col("max_close")).alias("is_local_top")])
daily_qqq = daily_qqq.rename({"row_count": "timestamp"})
daily_qqq.write_parquet("processed_daily_qqq.parquet")

v_conditions = [
    ('a', "a.is_local_top"),
    ('b', "b.is_local_bottom and b.close < a.close * 0.995"),
    ('c', "c.close > a.close")
]

ascending_triangles_conditions =  [('a', "a.is_local_bottom"), # first bottom 
     ('b', """b.is_local_top and b.close > a.close * 1.0025"""), # first top
     ('c', """c.is_local_bottom and c.close < b.close * 0.9975 and c.close > a.close * 1.0025"""), # second bottom, must be higher than first bottom
     ('d', """d.is_local_top and d.close > c.close * 1.0025 and abs(d.close / b.close) < 1.0025"""), # second top, must be similar to first top
     ('e', """e.is_local_bottom and e.close < d.close * 0.9975 and e.close > (c.close - a.close) / (c.timestamp - a.timestamp) * (e.timestamp - a.timestamp) + a.close"""), # third bottom, didn't break support
     ('f', """f.close > d.close * 1.0025""") #breakout resistance
]

heads_and_shoulders_conditions = [('a', "a.is_local_top"), # first shoulder
        ('b', """b.is_local_bottom and b.close < a.close * 0.997"""), # first bottom
        ('c', "c.is_local_top and c.close > a.close * 1.003"), # head
        ('d', "d.is_local_bottom and d.close < a.close * 0.997"), # second bottom
        ("e", "e.is_local_top and e.close > d.close * 1.003 and e.close < c.close * 0.997"), # second shoulder
        ("f", "f.close < ((d.close - b.close) / (d.timestamp - b.timestamp) * (f.timestamp - b.timestamp) + b.close) * 0.997"), # neckline
    ]

# cup_and_handle_conditions = [
#     ('a', 'a.is_local_top'),
#     ('b', 'b.is_local_bottom and b.close < a.close * 0.99'), #cup low
#     ('c', 'c.is_local_top and c.close > b.close * 1.01'), 
#     ('d', 'd.is_local_bottom and d.close < c.close * 0.99 and d.close > b.close * 1.01'), #handle low must be higher than cup low
#     ('e', 'e.close > (c.close - a.close) / (c.timestamp - a.timestamp) * (e.timestamp - a.timestamp) + a.close and (e.timestamp - c.timestamp) < (c.timestamp - a.timestamp) * 0.6'), #breakout
# ]

cup_and_handle_conditions = [
    ('a', 'a.is_local_top'),
    ('b', 'b.is_local_bottom and b.close < a.close * 0.99'), #cup low
    ('c', 'c.is_local_top and c.close > b.close * 1.01'), 
    ('d', 'd.is_local_bottom and d.close < c.close * 0.99 and d.close > b.close * 1.01'), #handle low must be higher than cup low
    ('e', 'e.close > (c.close - a.close) / (c.timestamp - a.timestamp) * (e.timestamp - a.timestamp) + a.close  and (e.timestamp - c.timestamp) < (c.timestamp - a.timestamp) * 0.6'), #breakout
]

udf_cup_and_handle = [
    ('a', 'a.is_local_top'),
    ('b', 'b.is_local_bottom and b.close < a.close * 0.99'), #cup low
    ('c', 'c.is_local_top and c.close > b.close * 1.01'), 
    ('d', 'd.is_local_bottom and d.close < c.close * 0.99 and d.close > b.close * 1.01'), #handle low must be higher than cup low
    ('e', 'e.close > lin_reg(a.close, a.timestamp, c.close, c.timestamp, e.timestamp)  and (e.timestamp - c.timestamp) < (c.timestamp - a.timestamp) * 0.6'), #breakout
]

def lin_reg(a_close, a_timestamp, c_close, c_timestamp, e_timestamp):
    return (c_close - a_close) / (c_timestamp - a_timestamp) * (e_timestamp - a_timestamp) + a_close 

# # plot_candlesticks(original_qqq)

# ascending_triangles = nfa_cep(qqq, ascending_triangles_conditions , "timestamp", 60 * 120)
# ascending_triangles = vector_interval_cep(qqq, ascending_triangles_conditions , "timestamp", 60 * 120)
# ascending_triangles = nfa_interval_cep_1(qqq, ascending_triangles_conditions , "timestamp", 60 * 120)

# print(ascending_triangles.unique("a_timestamp"))

# cup_and_handles = nfa_cep(daily_qqq, udf_cup_and_handle , "timestamp", 30, by = "symbol", udfs = {"lin_reg": lin_reg})
# cup_and_handles = nfa_cep(daily_qqq, cup_and_handle_conditions , "timestamp", 30, by = "symbol")
# cup_and_handles = vector_interval_cep(daily_qqq, cup_and_handle_conditions , "timestamp", 30, by = "symbol")

# minutely = polars.read_parquet("filtered_combined.parquet")
# cup_and_handles = nfa_cep(minutely, cup_and_handle_conditions , "timestamp", 7200, by = "symbol")

data = daily_qqq
conditions = [ascending_triangles_conditions]
strategies = [nfa_cep, vector_interval_cep]
span = 30
by = "symbol"

for condition in conditions:
    for strategy in strategies:
        results = strategy(data, condition, "timestamp", span, by=  by)
        print(results.unique(["a_timestamp", by]).sort(by))
        verify(data, results.unique(["a_timestamp", by]), condition)

# cup_and_handles.write_parquet("cup_and_handles.parquet")

# heads_and_shoulders = nfa_cep(qqq, heads_and_shoulders_conditions , "timestamp", 60 * 120)
# print(heads_and_shoulders.unique("a___row_count__"))

# v_shape = nfa_cep(qqq, v_conditions , "timestamp", 60 * 100)
# print(v_shape.unique("a___row_count__"))

# we can only verify that the things found satisfy the conditions, but we cannot verify that we found all the things that satisfy the conditions

