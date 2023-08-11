from nfa_cep import nfa_cep
from interval_nfa_cep_polars import nfa_interval_cep_polars
from interval_nfa_cep_sqlite import nfa_interval_cep_sqlite
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

    if not polars.SQLContext(frame = events).execute("select count(*) from frame where {}".format(all_predicate)).collect()["count"][0] == len(results):
        print("Verification failed, failed rows written to problem.parquet")
        polars.SQLContext(frame = events).execute("select * from frame where not ({})".format(all_predicate)).collect().write_parquet("problem.parquet")

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
daily_qqq = daily_qqq.groupby_rolling("row_count", period = "10i", offset = "-5i", by = "symbol", check_sorted = False).agg([
        polars.col("close").min().alias("min_close"),
        polars.col("close").max().alias("max_close"),
    ])\
    .hstack(daily_qqq.select(["close"]))\
    .with_columns([(polars.col("close") == polars.col("min_close")).alias("is_local_bottom"),
                  (polars.col("close") == polars.col("max_close")).alias("is_local_top")])
daily_qqq = daily_qqq.rename({"row_count": "timestamp"})
daily_qqq.write_parquet("processed_daily_qqq.parquet")

# daily_qqq = polars.read_parquet("daily.parquet")
# filtered_symbols = daily_qqq.groupby("symbol").agg([polars.count()]).filter(polars.col("count") == 252).select(["symbol"])
# daily_qqq = filtered_symbols.join(daily_qqq, "symbol")
# daily_qqq = daily_qqq.sort(["symbol", "date"])
# daily_qqq = polars.concat([i.with_row_count("row_count") for i in daily_qqq.partition_by("symbol")]).with_columns(polars.col("row_count").cast(polars.Int64()))
# daily_qqq = daily_qqq.groupby_rolling("row_count", period = "5i", by = "symbol", check_sorted = False).agg([
#         polars.col("close").mean().alias("rolling_5d_mean")]).hstack(daily_qqq.select(["close", "high", "low"]))
# daily_qqq = daily_qqq.rename({"row_count": "timestamp"})
# daily_qqq.write_parquet("david_daily_qqq.parquet")

v_conditions = [
    ('a', "a.is_local_top"),
    ('b', "b.is_local_bottom and b.close < a.close * LOWER"),
    ('c', "c.close > a.close")
]

ascending_triangles_conditions =  [('a', "a.is_local_bottom"), # first bottom 
     ('b', """b.is_local_top and b.close > a.close * UPPER"""), # first top
     ('c', """c.is_local_bottom and c.close < b.close * LOWER and c.close > a.close * UPPER"""), # second bottom, must be higher than first bottom
     ('d', """d.is_local_top and d.close > c.close * UPPER and abs(d.close / b.close) < UPPER"""), # second top, must be similar to first top
     ('e', """e.is_local_bottom and e.close < d.close * LOWER and e.close > (c.close - a.close) / (c.timestamp - a.timestamp) * (e.timestamp - a.timestamp) + a.close"""), # third bottom, didn't break support
     ('f', """f.close > d.close * UPPER""") #breakout resistance
]

descending_triangles_conditions = [('a', "a.is_local_top"), # first top
        ('b', """b.is_local_bottom and b.close < a.close * LOWER"""), # first bottom
        ('c', """c.is_local_top and c.close > b.close * UPPER and c.close < a.close * LOWER"""), # second top, must be lower than first top
        ('d', """d.is_local_bottom and d.close < c.close * LOWER and abs(d.close / b.close) < UPPER"""), # second bottom, must be similar to first bottom
        ('e', """e.is_local_top and e.close > d.close * UPPER and e.close < (c.close - a.close) / (c.timestamp - a.timestamp) * (e.timestamp - a.timestamp) + a.close"""), # third top, didn't break resistance
        ('f', """f.close < d.close * LOWER""") #breakout support
    ]

symmetrical_triangles_conditions_1 = [('a', "a.is_local_top"), # first top
        ('b', """b.is_local_bottom and b.close < a.close * LOWER"""), # first bottom
        ('c', """c.is_local_top and c.close > b.close * UPPER and c.close < a.close * LOWER"""), # second top, must be lower than first top
        ('d', """d.is_local_bottom and d.close < c.close * LOWER and d.close > b.close * UPPER"""), # second bottom, must be similar to first bottom
        ('e', """e.is_local_top and e.close > d.close * UPPER and e.close < (c.close - a.close) / (c.timestamp - a.timestamp) * (e.timestamp - a.timestamp) + a.close"""), # third top, didn't break resistance
        ('f', """f.close < d.close * LOWER""") #breakout support
    ]

symmetrical_triangles_conditions_2 = [('a', "a.is_local_bottom"), # first bottom
        ('b', """b.is_local_top and b.close > a.close * UPPER"""), # first top
        ('c', """c.is_local_bottom and c.close < b.close * LOWER and c.close > a.close * UPPER"""), # second bottom, must be higher than first bottom
        ('d', """d.is_local_top and d.close > c.close * UPPER and d.close < b.close * LOWER"""), # second top, must be similar to first top
        ('e', """e.is_local_bottom and e.close < d.close * LOWER and e.close > (c.close - a.close) / (c.timestamp - a.timestamp) * (e.timestamp - a.timestamp) + a.close"""), # third bottom, didn't break support
        ('f', """f.close > d.close * UPPER""") #breakout resistance
    ]

flag_1 = [('a', "a.is_local_top"), # first top
        ('b', """b.is_local_bottom and b.close < a.close * LOWER"""), # first bottom
        ('c', """c.is_local_top and c.close > b.close * UPPER"""), # second top, must be lower than first top
        ('d', """d.is_local_bottom and d.close < c.close * LOWER and d.close > b.close * UPPER"""), # second bottom, must be similar to first bottom
        ('e', """e.is_local_top and e.close > d.close * UPPER and e.close < (c.close - a.close) / (c.timestamp - a.timestamp) * (e.timestamp - a.timestamp) + a.close"""), # third top, didn't break resistance
        ('f', """f.close < d.close * LOWER""") #breakout support
    ]

test_1 = [('x0', 'x0.high < x0.rolling_5d_mean'),
 ('x1', 'x1.high < x1.rolling_5d_mean and x1.timestamp = x0.timestamp + 1'),
 ('x2', 'x2.high < x2.rolling_5d_mean and x2.timestamp = x1.timestamp + 1'),
 ('x3', 'x3.high < x3.rolling_5d_mean and x3.timestamp = x2.timestamp + 1'),
 ('x4', 'x4.high < x4.rolling_5d_mean and x4.timestamp = x3.timestamp + 1'),
 ('x5','x5.high > x5.rolling_5d_mean and x5.timestamp = x4.timestamp + 1 and x5.timestamp = 41')]

test_2 = [('x0', 'x0.high < x0.rolling_5d_mean'),
 ('x1', 'x1.high < x1.rolling_5d_mean and x1.timestamp = x0.timestamp + 1'),
 ('x2', 'x2.high > x2.rolling_5d_mean and x2.timestamp = x1.timestamp + 1')]

test_3 = [('x0', 'x0.low > x0.rolling_5d_mean'),
 ('x1', 'x1.low > x1.rolling_5d_mean and x1.timestamp = x0.timestamp + 1'),
 ('x2', 'x2.low > x2.rolling_5d_mean and x2.timestamp = x1.timestamp + 1'),
 ('x3', 'x3.low > x3.rolling_5d_mean and x3.timestamp = x2.timestamp + 1'),
 ('x4', 'x4.low > x4.rolling_5d_mean and x4.timestamp = x3.timestamp + 1'),
 ('x5','x5.close < x5.rolling_5d_mean and x5.timestamp = x4.timestamp + 1 and x5.timestamp = 51')]

heads_and_shoulders_conditions = [('a', "a.is_local_top"), # first shoulder
        ('b', """b.is_local_bottom and b.close < a.close * LOWER"""), # first bottom
        ('c', "c.is_local_top and c.close > a.close * UPPER"), # head
        ('d', "d.is_local_bottom and d.close < a.close * LOWER"), # second bottom
        ("e", "e.is_local_top and e.close > d.close * UPPER and e.close < c.close * LOWER"), # second shoulder
        ("f", "f.close < ((d.close - b.close) / (d.timestamp - b.timestamp) * (f.timestamp - b.timestamp) + b.close) * LOWER"), # neckline
    ]

cup_and_handle_conditions = [
    ('a', 'a.is_local_top'),
    ('b', 'b.is_local_bottom and b.close < a.close * LOWER'), #cup low
    ('c', 'c.is_local_top and c.close > b.close * UPPER'), 
    ('d', 'd.is_local_bottom and d.close < c.close * LOWER and d.close > b.close * UPPER'), #handle low must be higher than cup low
    ('e', 'e.close > (c.close - a.close) / (c.timestamp - a.timestamp) * (e.timestamp - a.timestamp) + a.close  and (e.timestamp - c.timestamp) < (c.timestamp - a.timestamp) * 0.6'), #breakout
]

udf_cup_and_handle = [
    ('a', 'a.is_local_top'),
    ('b', 'b.is_local_bottom and b.close < a.close * LOWER'), #cup low
    ('c', 'c.is_local_top and c.close > b.close * UPPER'), 
    ('d', 'd.is_local_bottom and d.close < c.close * LOWER and d.close > b.close * UPPER'), #handle low must be higher than cup low
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

data = qqq
conditions = [udf_cup_and_handle]
strategies = [ ("nfa_cep", nfa_cep)]
span = 7200
by = None

UPPER = 1.0025
LOWER = 0.9975

for condition in conditions:

    for i in range(len(condition)):
        if condition[i][1] is not None:
            condition[i] = (condition[i][0],condition[i][1].replace("UPPER", str(UPPER)).replace("LOWER", str(LOWER)) )

    for strategy_name, strategy in strategies:
        print("USING STRATEGY {}".format(strategy_name))
        results = strategy(data, condition, "timestamp", span, by= by)
        # print(results)

        if by is not None:
            results.unique([condition[0][0] + "_timestamp", by]).sort(by).write_parquet(strategy_name + ".parquet")
            print(results.unique([condition[0][0] + "_timestamp", by]).sort(by))
            verify(data, results.unique([condition[0][0] + "_timestamp", by]), condition)
        
        else:
            results.unique(condition[0][0] + "_timestamp").sort(condition[0][0] + "_timestamp").write_parquet(strategy_name + ".parquet")
            print(results.unique(condition[0][0] + "_timestamp").sort(condition[0][0] + "_timestamp"))
            verify(data, results.unique(condition[0][0] + "_timestamp"), condition)

# cup_and_handles.write_parquet("cup_and_handles.parquet")

# heads_and_shoulders = nfa_cep(qqq, heads_and_shoulders_conditions , "timestamp", 60 * 120)
# print(heads_and_shoulders.unique("a___row_count__"))

# v_shape = nfa_cep(qqq, v_conditions , "timestamp", 60 * 100)
# print(v_shape.unique("a___row_count__"))

# we can only verify that the things found satisfy the conditions, but we cannot verify that we found all the things that satisfy the conditions

