import os
os.environ["POLARS_MAX_THREADS"] = "1" 
import polars, sqlglot, duckdb
import sqlglot.optimizer as optimizer
from collections import deque
from collections import namedtuple
import time
from functools import partial
from typing import List, Tuple, Dict, Callable, Any, Union, Optional

def verify(data, results, conditions):
    # the results should have event_name__row_count__ column

    all_predicate = ""
    events = []
    for condition in conditions:
        event_name, predicate = condition
        if predicate is not None:
            all_predicate += "({}) and ".format(predicate)
        event_nrs = results[event_name + "___row_count__"].to_list()
        events.append(data[event_nrs].rename({col : event_name + "_" + col for col in data[event_nrs].columns}))
    
    events = polars.concat(events, how = "horizontal")

    events.write_parquet("events.parquet")

    all_predicate = all_predicate[:-5]
    all_predicate = sqlglot.parse_one(all_predicate).transform(lambda node: sqlglot.exp.column(node.table + "_" + node.name) if isinstance(node, sqlglot.exp.Column) else node).sql()
    
    print(all_predicate)

    con = duckdb.connect()
    events_arrow = events.to_arrow()
    result = polars.from_arrow(con.execute("select count(*) from events_arrow where {}".format(all_predicate)).arrow())["count_star()"][0]
    
    if not result == len(results):
        polars.from_arrow(con.execute("select * from events_arrow where not ({})".format(all_predicate)).arrow()).write_parquet("problem.parquet")

    # if not polars.SQLContext(frame = events).execute("select count(*) from frame where {}".format(all_predicate)).collect()["count"][0] == len(results):
    #     print("Verification failed, failed rows written to problem.parquet")
    #     polars.SQLContext(frame = events).execute("select * from frame where not ({})".format(all_predicate)).collect().write_parquet("problem.parquet")

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

def touched_columns(conditions):
    result = set()
    for event_name, condition in conditions:
        result = result.union(set([i.name for i in sqlglot.parse_one(condition).find_all(sqlglot.expressions.Column)]))
    return result

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
    # print(prefilter)
    # print(touched_columns)
    # print(event_prefilters)
    # print(event_dependent_filters)
    # print(event_required_columns)
    return prefilter, touched_columns, event_prefilters, event_dependent_filters, event_required_columns

def partial_any_arg(func, value_locs):
    def wrapper(tup):
        new_args = list(tup)
        for i in sorted(value_locs.keys()):
            new_args.insert(i, value_locs[i])
        return func(*new_args)

    return partial(wrapper)

def preprocess_2(batch: polars.DataFrame, events: List[Tuple], time_col: str, by: Union[str, List,None], udfs: Dict[str,Callable], max_span: int):

    assert type(batch) == polars.DataFrame, "batch must be a polars DataFrame"
    if by is None:
        assert batch[time_col].is_sorted(), "batch must be sorted by time_col"
    else:
        if type(by) == str:
            by = [by]
        assert all([col in batch.columns for col in by]), "groupby columns must be in batch"
        batch = batch.sort(by)

    event_names = [event for event, predicate in events]
    prefilter, touched_columns, event_prefilters, event_predicates, event_required_columns = preprocess_conditions(events)

    udf_required_columns = {event: [] for event in event_names}

    udf_set = set()
    for udf in udfs:
        if udf.lower() in udf_set:
            raise Exception("UDF names are case insensitive. You cannot define both LIN_REG and lin_reg for example.")
        udf_set.add(udf.lower())

    # preprocess UDFs

    event_udfs = {event: [] for event in event_names}
    for event in event_predicates:
        predicate = event_predicates[event]
        if predicate is None:
            continue
        my_udfs = list(sqlglot.parse_one(predicate).find_all(sqlglot.exp.Anonymous))
        for udf in my_udfs:
            name = udf.this # note this name will be upper case by default!
            if name.lower() in udfs:
                udf_func = udfs[name.lower()]
            elif name in udfs:
                udf_func = udfs[name]
            elif name.upper() in udfs:
                udf_func = udfs[name.upper()]
            else:
                raise Exception("udf {} not in specified udf dictionary".format(name))

            current_arguments = []
            prior_arguments = []
            counter = 0
            for col in udf.expressions:
                assert type(col) == sqlglot.exp.Column, "only non-modified column arguments are supported, i.e. you cannot do udf(x + 1)"
                col_event = col.table
                col_name = col.name
                if col_event == event:
                    current_arguments.append((counter, col_name))
                else:
                    prior_arguments.append((col_event, col_name))
                    udf_required_columns[col_event].append(col_name)
                counter += 1
            
            # the predicate will automatically upper case the udf name
            event_udfs[event].append((name.upper(), udf_func, current_arguments, prior_arguments))
    
        event_predicates[event] = sqlglot.parse_one(predicate).transform(lambda node: sqlglot.parse_one(node.this) if isinstance(node, sqlglot.exp.Anonymous) else node).sql()
    

    for event in event_names:
        if event not in event_required_columns:
            event_required_columns[event] = {time_col}
        else:
            event_required_columns[event].add(time_col)
        for udf_required_col in udf_required_columns[event]:
            event_required_columns[event].add(udf_required_col)

    # make sure these columns have a deterministic order  
    event_required_columns = {event: ["__row_count__"] + list(event_required_columns[event]) for event in event_names}

    select_cols = touched_columns.union({time_col}) if by is None else touched_columns.union({time_col, *by})

    # apply prefilter

    batch = batch.with_row_count("__original_row_count__")

    batch = polars.SQLContext(frame=batch).execute("select __original_row_count__, {} from frame where {}".format(",".join(select_cols), prefilter)).collect()

    batch = batch.with_row_count("__row_count__")
    row_count_mapping = batch.select(["__original_row_count__", "__row_count__"])
    batch = batch.drop(["__original_row_count__"])

    event_independent_columns = {event: [k.name for k in sqlglot.parse_one(predicate).find_all(sqlglot.exp.Column) if k.table == event] 
                               for event, predicate in events if predicate is not None}

    event_rename_dicts = {event: {k.sql() : k.table + "_" + k.name for k in sqlglot.parse_one(predicate).find_all(sqlglot.exp.Column)} for event, predicate in events if predicate is not None}
    
    # we need to rename the predicates to use _ instead of . because polars doesn't support . in column names
    event_predicates = [replace_with_dict(predicate, event_rename_dicts[event]) if (predicate is not None) else None for event, predicate in event_predicates.items()]
    # print(event_prefilters)
    # print(event_predicates)
    rename_dicts = {event: {col: event + "_" + col for col in batch.columns} for event in event_names}
    
    assert (event_predicates[0] == "TRUE" or event_predicates[0] == None) and all([predicate is not None for predicate in event_predicates[1:]]), \
        "only first event can be None"  

    # compute event indices.

    event_indices = {event_name: None for event_name in event_names}
    for event_name in event_names:
        if event_prefilters[event_name] != "TRUE":
            if by is None:
                event_indices[event_name] = polars.SQLContext().register(event_name, batch).execute("select __row_count__, {} from {} where {}".
                                                            format(time_col, event_name, event_prefilters[event_name])).collect()
            else:
                event_indices[event_name] = polars.SQLContext().register(event_name, batch).execute("select __row_count__, {}, {} from {} where {}".
                                                            format(time_col, ",".join(by), event_name, event_prefilters[event_name])).collect()
        else:
            event_indices[event_name] = None
    
    if event_indices[event_names[0]] is not None:

        # we are going to prune the event_indices of the first event to only intervals where there exists at least one of the other events
        for event_name in event_names[1:]:
            if event_indices[event_name] is not None:
                # do the asof join to find the closest event to the first event
                event_indices[event_names[0]] = event_indices[event_names[0]].join_asof(event_indices[event_name], on = "__row_count__", by = by, strategy = "forward")\
                    .filter(polars.col(time_col + "_right") - polars.col(time_col) <= max_span).select(["__row_count__", time_col] + (by if by is not None else []))

    for event_name in event_names:
        if event_prefilters[event_name] != "TRUE":
            event_indices[event_name] = set(event_indices[event_name]["__row_count__"])
        
    # print(len(event_indices[event_names[0]]))

    # compute intervals
    if event_indices[event_names[0]] is not None:
        start_rows = batch.select(["__row_count__", time_col]+ (by if by is not None else [])).join(polars.from_dict({"row_nrs": list(event_indices[event_names[0]])}).
                            with_columns(polars.col("row_nrs").cast(polars.UInt32())), left_on = "__row_count__", right_on = "row_nrs", how = "semi")
    else:
        # likely to be slow, no filter on first event
        start_rows = batch.select(["__row_count__", time_col]+ (by if by is not None else []))
    end_times = start_rows.with_columns(polars.col(time_col) + max_span)
    # perform as asof join to figure out the end_rows
    end_rows = end_times.set_sorted(time_col).\
        join_asof(batch.select(["__row_count__", time_col] + (by if by is not None else [])).set_sorted(time_col), 
                  left_on = time_col, right_on = time_col, strategy = "backward", by = by)
    intervals = end_rows.with_columns([
        polars.col("__row_count__").alias("__arc__"),
        polars.col("__row_count___right").alias("__crc__"),
    ])
    
    return batch, event_names, rename_dicts, event_predicates, event_indices, event_independent_columns, event_required_columns, event_udfs, intervals, row_count_mapping



def process_matched_events(batch: polars.DataFrame, matched_events, row_count_mapping, event_names, time_col, by: Union[str, List,None], total_events):
    
    if type(by) == str:
        by = [by]
    
    if len(matched_events) > 0:

        matched_events = [item for sublist in matched_events for item in sublist]
        matched_events = batch[matched_events].select(["__row_count__", time_col] + by) if by is not None else batch[matched_events].select(["__row_count__", time_col])
        
        # use left join to preserve the order of matched_events.
        
        matched_events = matched_events.join(row_count_mapping, on = "__row_count__", how = "left").drop(["__row_count__"]).rename({"__original_row_count__": "__row_count__"})

        events = [matched_events[i::total_events] for i in range(total_events)]
        for i in range(total_events):
            if i != 0 and by is not None:
                events[i] = events[i].drop(by)
            events[i] = events[i].rename({"__row_count__" : event_names[i] + "___row_count__", time_col : event_names[i] + "_" + time_col})
        matched_events = polars.concat(events, how = 'horizontal')
        return matched_events
    
    else:
        return None