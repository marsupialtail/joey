import polars, sqlglot
import sqlglot.optimizer as optimizer
from collections import deque
from collections import namedtuple
from tqdm import tqdm
import time
from functools import partial

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

def partial_any_arg(func, value_locs):
    def wrapper(tup):
        new_args = list(tup)
        for i in sorted(value_locs.keys()):
            new_args.insert(i, value_locs[i])
        return func(*new_args)

    return partial(wrapper)

def preprocess_2(batch, events, time_col, by, udfs):
    event_names = [event for event, predicate in events]
    prefilter, touched_columns, event_prefilters, event_predicates, event_required_columns = preprocess_conditions(events)

    udf_required_columns = {event: [] for event in event_names}

    udf_set = set()
    for udf in udfs:
        if udf.lower() in udf_set:
            raise Exception("UDF names are case insensitive. You cannot define both LIN_REG and lin_reg for example.")
        udf_set.add(udf.lower())

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

    select_cols = touched_columns.union({time_col}) if by is None else touched_columns.union({time_col, by})
    batch = polars.SQLContext(frame=batch).execute("select {} from frame where {}".format(",".join(select_cols), prefilter)).collect()

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

    event_indices = {event_name: None for event_name in event_names}
    for event_name in event_names:
        if event_prefilters[event_name] != "TRUE":
            event_indices[event_name] = set(polars.SQLContext().register(event_name, batch).execute("select __row_count__ from {} where {}".
                                                            format(event_name, event_prefilters[event_name])).collect()["__row_count__"])
        else:
            event_indices[event_name] = None
    
    return batch, event_names, rename_dicts, event_predicates, event_indices, event_independent_columns, event_required_columns, event_udfs