import polars, sqlglot, duckdb
import sqlglot.optimizer as optimizer
from collections import deque
from collections import namedtuple
from tqdm import tqdm
import time

def plot_candlesticks(prices):
    import matplotlib.pyplot as plt
    prices = prices.to_pandas()
    #create figure
    plt.figure()

    #define width of candlestick elements
    width = .4
    width2 = .05

    #define up and down prices
    up = prices[prices.close>=prices.open]
    down = prices[prices.close<prices.open]

    #define colors to use
    col1 = 'green'
    col2 = 'red'

    #plot up prices
    plt.bar(up.index,up.close-up.open,width,bottom=up.open,color=col1)
    plt.bar(up.index,up.high-up.close,width2,bottom=up.close,color=col1)
    plt.bar(up.index,up.low-up.open,width2,bottom=up.open,color=col1)

    #plot down prices
    plt.bar(down.index,down.close-down.open,width,bottom=down.open,color=col2)
    plt.bar(down.index,down.high-down.open,width2,bottom=down.open,color=col2)
    plt.bar(down.index,down.low-down.close,width2,bottom=down.close,color=col2)

    #rotate x-axis tick labels
    plt.xticks(rotation=45, ha='right')

    #display candlestick chart
    plt.show()

def remove_qualifier(query):
    return sqlglot.parse_one(query).transform(lambda node: sqlglot.exp.column(node.this) if isinstance(node, sqlglot.exp.Column) else node).sql()

def repeat_row(row, n):
    assert len(row) == 1
    # repeat a polars dataframe row n times
    return row.select([polars.col(col).repeat_by(n).explode() for col in row.columns])

def replace_with_dict(string, d):
    for k in d:
        string = string.replace(k, d[k])
    return string

def preprocess_conditions(events):
    touched_columns = set()
    event_required_columns = {}
    prefilter = sqlglot.exp.FALSE
    seen_events = set()
    event_prefilters = {}
    event_dependent_filters = {}
    
    # we need this for the filter condition in the range join. We can only include the part of the 
    # last event's event_dependent_filter that's dependent on the first event
    last_event_dependent_on_first_event_filter = sqlglot.exp.TRUE
    
    # we need this for the selection of the range join.
    for i in range(len(events)):
        event_prefilter = sqlglot.exp.TRUE
        event_dependent_filter = sqlglot.exp.TRUE
        event_name, event_filter = events[i]

        if i != 0:
            assert event_filter is not None and event_filter != sqlglot.exp.TRUE, "only the first event can have no filter"

        assert event_name not in seen_events, "repeated event names not allowed"
        if event_filter is not None:
            event_filter = sqlglot.parse_one(event_filter)
            conjuncts = list(event_filter.flatten() if isinstance(event_filter, sqlglot.exp.And) else [event_filter])
            for conjunct in conjuncts:

                all_columns = list(conjunct.find_all(sqlglot.expressions.Column))
                conjunct_dependencies = set(i.table for i in all_columns)
                conjunct_columns = set(i.name for i in all_columns)
                touched_columns = touched_columns.union(conjunct_columns)
                assert '' not in conjunct_dependencies, "must specify the table name in columns, like a.x instead of x"
                assert conjunct_dependencies.issubset(seen_events.union({event_name})), "an event can only depend on itself or prior events"
                                
                if len(conjunct_dependencies) == 0:
                    raise Exception
                elif len(conjunct_dependencies) == 1 and list(conjunct_dependencies)[0] == event_name:
                    event_prefilter = sqlglot.exp.and_(event_prefilter, conjunct)
                else:
                    event_dependent_filter = sqlglot.exp.and_(event_dependent_filter, conjunct)
                    
                    if len(conjunct_dependencies) == 1 and list(conjunct_dependencies)[0] == events[0][0]:
                        if i == len(events) - 1:
                            last_event_dependent_on_first_event_filter = sqlglot.exp.and_(
                                last_event_dependent_on_first_event_filter, conjunct)
                
                for i in all_columns:
                    if i.table != event_name:
                        event_required_columns[i.table] = event_required_columns.get(i.table, set()).union({i.name})
                    
        event_prefilter = optimizer.simplify.simplify(event_prefilter)
        event_prefilters[event_name] = event_prefilter.sql()
        event_dependent_filters[event_name] = optimizer.simplify.simplify(event_dependent_filter).sql()
        prefilter = sqlglot.exp.or_(prefilter, event_prefilter)
        seen_events.add(event_name)
        
    
    prefilter = optimizer.simplify.simplify(prefilter).sql()
    prefilter = remove_qualifier(prefilter)
    last_event_dependent_on_first_event_filter = optimizer.simplify.simplify(last_event_dependent_on_first_event_filter).sql()
    print(prefilter)
    print(touched_columns)
    print(event_prefilters)
    print(event_dependent_filters)
    print(event_required_columns)
    return prefilter, touched_columns, event_prefilters, event_dependent_filters, event_required_columns

def nfa_cep(batch, events, time_col, max_span):
    
    assert type(batch) == polars.DataFrame, "batch must be a polars DataFrame"
    assert batch[time_col].is_sorted(), "batch must be sorted by time_col"

    event_names = [event for event, predicate in events]
    prefilter, touched_columns, event_prefilters, event_predicates, event_required_columns = preprocess_conditions(events)

    for event in event_names:
        if event not in event_required_columns:
            event_required_columns[event] = {time_col}
        else:
            event_required_columns[event].add(time_col)

    batch = polars.SQLContext(frame=batch).execute("select {} from frame where {}".format(",".join(touched_columns.union({time_col})), prefilter)).collect()

    batch = batch.with_row_count("__row_count__")

    event_independent_columns = {event: [k.name for k in sqlglot.parse_one(predicate).find_all(sqlglot.exp.Column) if k.table == event] 
                               for event, predicate in events if predicate is not None}

    event_rename_dicts = {event: {k.sql() : k.table + "_" + k.name for k in sqlglot.parse_one(predicate).find_all(sqlglot.exp.Column)} for event, predicate in events if predicate is not None}
    
    # we need to rename the predicates to use _ instead of . because polars doesn't support . in column names
    event_predicates = [replace_with_dict(predicate, event_rename_dicts[event]) if (predicate is not None and predicate != 'TRUE') else None for event, predicate in event_predicates.items()]
    print(event_predicates)
    rename_dicts = {event: {col: event + "_" + col for col in batch.columns} for event in event_names}
    
    assert (event_predicates[0] == "TRUE" or event_predicates[0] == None) and all([predicate is not None for predicate in event_predicates[1:]]), \
        "only first event can be None"
    
    total_len = len(batch)
    total_events = len(events)

    # sequences = polars.from_dicts([{"seq_id": 0, "start_time": batch[time_col][0]}])
    matched_sequences = {i: None for i in range(total_events)}
    matched_sequences[0] = batch[0].rename(rename_dicts[event_names[0]]).select([event_names[0] + "_" + k for k in event_required_columns[event_names[0]]])
    
    event_indices = {event_name: None for event_name in event_names}
    for event_name in event_names:
        if event_prefilters[event_name] != "TRUE":
            event_indices[event_name] = set(polars.SQLContext().register(event_name, batch).execute("select __row_count__ from {} where {}".
                                                            format(event_name, event_prefilters[event_name])).collect()["__row_count__"])
        else:
            event_indices[event_name] = None
    
    total_filter_time = 0
    total_other_time = 0

    for row in tqdm(range(total_len)):
        current_time = batch[time_col][row]
        this_row_can_be = [i for i in range(1, total_events) if event_indices[event_names[i]] is None or row in event_indices[event_names[i]]]
        
        # if row % 1000 == 0:
        #     print(total_filter_time, total_other_time)
        
        for seq_len in this_row_can_be:
            
            predicate = event_predicates[seq_len]
            assert predicate is not None
            for col in event_independent_columns[event_names[seq_len]]:
                predicate = predicate.replace(event_names[seq_len] + "_" + col, str(batch[col][row]))
            # predicate += " and a_timestamp >= {}".format(current_time - max_span)
            # print(predicate)

            # evaluate the predicate against matched_sequences[seq_len - 1]
            if matched_sequences[seq_len - 1] is not None and len(matched_sequences[seq_len - 1]) > 0:
                # print(matched_sequences)      
                start = time.time()
                matched_sequences[seq_len - 1] = matched_sequences[seq_len - 1].filter(polars.col(event_names[seq_len - 1] + "_" + time_col) >= current_time - max_span)
                total_filter_time += time.time() - start

                # print("{}".format(predicate))
                start = time.time()           
                matched = polars.SQLContext(frame=matched_sequences[seq_len - 1]).execute(
                    "select * from frame where {}".format(predicate)).collect()
                # print(time.time() - start)

                # now horizontally concatenate your table against the matched
                if len(matched) > 0:
                    matched = matched.hstack(repeat_row(batch[row].rename(rename_dicts[event_names[seq_len]])
                                                        .select([event_names[seq_len] + "_" + k for k in event_required_columns[event_names[seq_len]]]), len(matched)))
                    if matched_sequences[seq_len] is None:
                        matched_sequences[seq_len] = matched
                    else:
                        matched_sequences[seq_len].vstack(matched, in_place=True)
                total_other_time += time.time() - start
        
        if event_indices[event_names[0]] is None:
            matched_sequences[0].vstack(batch[row].rename(rename_dicts[event_names[0]]).select([event_names[0] + "_" + k for k in event_required_columns[event_names[0]]]), in_place=True)
        else:
            if row in event_indices[event_names[0]]:
                matched_sequences[0].vstack(batch[row].rename(rename_dicts[event_names[0]]).select([event_names[0] + "_" + k for k in event_required_columns[event_names[0]]]), in_place=True)
    
    if matched_sequences[total_events - 1] is None:
        return None
    else:
        return matched_sequences[total_events - 1].filter(polars.col(event_names[-1] + "_" + time_col) - polars.col(event_names[0] + "_" + time_col) <= max_span)

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


# # plot_candlesticks(original_qqq)

ascending_triangles = nfa_cep(qqq, ascending_triangles_conditions , "timestamp", 60 * 120)
print(ascending_triangles.unique("a_timestamp"))

# heads_and_shoulders = nfa_cep(qqq, heads_and_shoulders_conditions , "timestamp", 60 * 120)
# print(heads_and_shoulders.unique("a___row_count__"))

# v_shape = nfa_cep(qqq, v_conditions , "timestamp", 60 * 100)
# print(v_shape.unique("a___row_count__"))