from utils import * 
import duckdb

def vector_interval_cep(batch, events, time_col, max_span, by = None, event_udfs = {}):
    
    assert type(batch) == polars.DataFrame, "batch must be a polars DataFrame"
    if by is None:
        assert batch[time_col].is_sorted(), "batch must be sorted by time_col"
    else:
        assert by in batch.columns

    batch, event_names, rename_dicts, event_predicates, event_indices, event_independent_columns, event_required_columns , event_udfs = preprocess_2(batch, events, time_col, by, event_udfs)
    # this hack needs to be corrected, this basically makes all the {event_name}_{col_name} col names just {col_name} for the current event
    for i in range(1, len(event_names)):
        for col in event_independent_columns[event_names[i]]:
            event_predicates[i] = event_predicates[i].replace(event_names[i] + "_" + col, col)

    total_events = len(events)

    assert event_indices[event_names[0]] != None, "this is for things with first event filter"

    end_results = []

    total_filter_time = 0
    total_other_time = 0
    counter = 0

    for i in range(1, total_events):
        if event_indices[event_names[i]] == None:
            batch = batch.with_columns(polars.lit(True).alias("__possible_{}__".format(event_names[i])))
            continue
        event_df = polars.from_dict({"event_nrs": list(event_indices[event_names[i]]) })\
            .with_columns([polars.col("event_nrs").cast(polars.UInt32()),
                polars.lit(True).alias("__possible_{}__".format(event_names[i]))])
        batch = batch.join(event_df, left_on = "__row_count__", right_on = "event_nrs", how = "left")

    partitioned = batch.partition_by(by) if by is not None else [batch]

    matched_ends = []
    total_exec_times = []
    total_section_lengths = []
    
    overhead = 0

    matched_events = []

    length_dicts = {event_name: [] for event_name in event_names}

    for batch in partitioned if by is None else tqdm(partitioned):
        
        start_rows = batch.select(["__row_count__", time_col]).join(polars.from_dict({"row_nrs": list(event_indices[event_names[0]])}).
                            with_columns(polars.col("row_nrs").cast(polars.UInt32())), left_on = "__row_count__", right_on = "row_nrs", how = "semi")
        end_times = start_rows.with_columns(polars.col(time_col) + max_span)
        assert end_times[time_col].is_sorted()
        # perform as asof join to figure out the end_rows
        end_rows = end_times.set_sorted(time_col).join_asof(batch.select(["__row_count__", time_col]).set_sorted(time_col), left_on = time_col, right_on = time_col, strategy = "backward")
        result = end_rows.with_columns([
            polars.col("__row_count__").alias("__arc__"),
            polars.col("__row_count___right").alias("__crc__"),
        ])
        
        start_row_count = batch["__row_count__"][0]
        for bound in (tqdm(result.to_dicts()) if by is None else result.to_dicts()):

            start_nr = bound["__arc__"]
            end_nr = bound["__crc__"]
            # start_ts = bound["__ats__"]
            # end_ts = bound["__cts__"]
            # print(start_nr)
            # match recognize default is one pattern per start. we can change it to one pattern per end too
            # if end_nr in matched_ends:

            if start_nr in matched_ends or end_nr <= start_nr:
                continue
            my_section = batch[start_nr - start_row_count: end_nr + 1 - start_row_count] #.to_arrow()
            fate = {event_names[0] + "_" + col : my_section[col][0] for col in my_section.columns}
            stack = deque([(0, my_section[0], [fate], [start_nr])])
            
            while stack:
                marker, vertex, path, matched_event = stack.pop()
            
                remaining_df = my_section[marker:]# .to_arrow()
                next_event_name = event_names[len(path)]
                next_event_filter = event_predicates[len(path)]
                
                # now fill in the next_event_filter based on the previous path
                for fate in path:
                    for fixed_col in fate:
                        next_event_filter = next_event_filter.replace(fixed_col, str(fate[fixed_col]))

                startt = time.time()
                query = "select {}, {}, close from frame where __possible_{}__ and {}".format("__row_count__", time_col, next_event_name, next_event_filter)
                matched = polars.SQLContext(frame=remaining_df).execute(query).collect()
                total_exec_times.append(time.time() - startt)
                length_dicts[next_event_name].append(len(remaining_df))
                startt = time.time()

                # total_exec_times.append(time.time() - startt)
                total_section_lengths.append(len(remaining_df))
                
                if len(matched) > 0:
                    if next_event_name == event_names[-1]:
                        matched_ends.append(start_nr)
                        matched_events.append(matched_event + [end_nr])
                        break
                    else:
                        for matched_row in matched.iter_rows(named = True):
                            my_fate = {next_event_name + "_" + col : matched_row[col] for col in matched_row}
                            # the row count of the matched row in the section is it's row count col value minus
                            # the start row count of the section
                            stack.append((matched_row["__row_count__"] - start_nr, matched_row, path + [my_fate], matched_event + [matched_row[time_col]]))
                overhead += time.time() - startt
    print(len(matched_events))
    print(sum(total_exec_times), len(total_exec_times))
    for key in length_dicts:
        print(key, len(length_dicts[key]), np.mean(length_dicts[key]))
    print(overhead)
    return matched_events