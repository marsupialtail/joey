from utils import * 
import duckdb

def vector_interval_cep(batch, events, time_col, max_span, by = None, event_udfs = {}):
    
    assert type(batch) == polars.DataFrame, "batch must be a polars DataFrame"
    if by is None:
        assert batch[time_col].is_sorted(), "batch must be sorted by time_col"
    else:
        assert by in batch.columns

    batch, event_names, rename_dicts, event_predicates, event_indices, event_independent_columns, event_required_columns , event_udfs = preprocess_2(batch, events, time_col, by, event_udfs, max_span)
    # this hack needs to be corrected, this basically makes all the {event_name}_{col_name} col names just {col_name} for the current event
    for i in range(1, len(event_names)):
        for col in event_independent_columns[event_names[i]]:
            event_predicates[i] = event_predicates[i].replace(event_names[i] + "_" + col, col)

    total_events = len(events)

    if event_indices[event_names[0]] == None:
        print("vectored cep is for things with first event filter, likely will be slow")

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

    for partition in partitioned if by is None else tqdm(partitioned):
        
        if event_indices[event_names[0]] is not None:
            start_rows = partition.select(["__row_count__", time_col]).join(polars.from_dict({"row_nrs": list(event_indices[event_names[0]])}).
                            with_columns(polars.col("row_nrs").cast(polars.UInt32())), left_on = "__row_count__", right_on = "row_nrs", how = "semi")
        else:
            # likely to be slow, no filter on first event
            start_rows = partition.select(["__row_count__", time_col])
        end_times = start_rows.with_columns(polars.col(time_col) + max_span)
        assert end_times[time_col].is_sorted()
        # perform as asof join to figure out the end_rows
        end_rows = end_times.set_sorted(time_col).join_asof(partition.select(["__row_count__", time_col]).set_sorted(time_col), left_on = time_col, right_on = time_col, strategy = "backward")
        result = end_rows.with_columns([
            polars.col("__row_count__").alias("__arc__"),
            polars.col("__row_count___right").alias("__crc__"),
        ])
        
        start_row_count = partition["__row_count__"][0]
        for bound in (tqdm(result.to_dicts()) if by is None else result.to_dicts()):

            start_nr = bound["__arc__"]
            end_nr = bound["__crc__"]

            # match recognize default is one pattern per start. we can change it to one pattern per end too
            # if end_nr in matched_ends:

            if start_nr in matched_ends or end_nr <= start_nr:
                continue
            my_section = partition[start_nr - start_row_count: end_nr + 1 - start_row_count] #.to_arrow()
            fate = {event_names[0] + "_" + col : my_section[col][0] for col in my_section.columns}
            stack = deque([(0, my_section[0], [fate], [start_nr])])
            
            while stack:
                marker, vertex, path, matched_event = stack.popleft()
            
                remaining_df = my_section[marker + 1:]# .to_arrow()
                next_event_name = event_names[len(path)]
                next_event_filter = event_predicates[len(path)]
                
                # now fill in the next_event_filter based on the previous path
                for fate in path:
                    for fixed_col in fate:
                        next_event_filter = next_event_filter.replace(fixed_col, str(fate[fixed_col]))

                startt = time.time()
                query = "select {}, {} from frame where __possible_{}__ and {}".format("__row_count__", ",".join(event_required_columns[next_event_name]), next_event_name, next_event_filter)
                matched = polars.SQLContext(frame=remaining_df).execute(query).collect()
                total_exec_times.append(time.time() - startt)
                length_dicts[next_event_name].append(len(remaining_df))
                startt = time.time()

                # total_exec_times.append(time.time() - startt)
                total_section_lengths.append(len(remaining_df))
                
                if len(matched) > 0:
                    if next_event_name == event_names[-1]:
                        matched_ends.append(start_nr)
                        matched_events.append(matched_event + [matched["__row_count__"][0]])
                        break
                    else:
                        for matched_row in matched.iter_rows(named = True):
                            my_fate = {next_event_name + "_" + col : matched_row[col] for col in matched_row}
                            # the row count of the matched row in the section is it's row count col value minus
                            # the start row count of the section
                            stack.appendleft((matched_row["__row_count__"] - start_nr, matched_row, path + [my_fate], matched_event + [matched_row["__row_count__"]]))
                overhead += time.time() - startt

    # now create a dataframe from the matched events, first flatten matched_events into a flat list
    if len(matched_events) > 0:
        matched_events = [item for sublist in matched_events for item in sublist]
        matched_events = batch[matched_events].select(["__row_count__", time_col, by]) if by is not None else batch[matched_events].select(["__row_count__", time_col])
        
        events = [matched_events[i::total_events] for i in range(total_events)]
        for i in range(total_events):
            if i != 0 and by is not None:
                events[i] = events[i].drop(by)
            events[i] = events[i].rename({"__row_count__" : event_names[i] + "___row_count__", time_col : event_names[i] + "_" + time_col})
        matched_events = polars.concat(events, how = 'horizontal')

        print("TIME SPENT IN FILTER {} {} ".format(sum(total_exec_times), len(total_exec_times)))
        for key in length_dicts:
            print(key, len(length_dicts[key]), np.mean(length_dicts[key]))
        
        print("TOTAL FILTER EVENTS: ", sum([len(length_dicts[key]) for key in length_dicts]))
        print("TOTAL FILTERED ROWS: ", sum([np.sum(length_dicts[key]) for key in length_dicts]))
        print(overhead)
        return matched_events
    else:
        return None