from utils import * 

def nfa_interval_cep_1(batch, events, time_col, max_span, by = None, event_udfs = {}):
    
    assert type(batch) == polars.DataFrame, "batch must be a polars DataFrame"
    if by is None:
        assert batch[time_col].is_sorted(), "batch must be sorted by time_col"
    else:
        assert by in batch.columns

    batch, event_names, rename_dicts, event_predicates, event_indices, event_independent_columns, event_required_columns , event_udfs = preprocess_2(batch, events, time_col, by, event_udfs)

    total_events = len(events)

    assert event_indices[event_names[0]] != None, "this is for things with first event filter"

    end_results = []

    total_filter_time = 0
    total_other_time = 0
    counter = 0

    partitioned = batch.partition_by(by) if by is not None else [batch]
    length_dicts = {event_name: [] for event_name in event_names}
    for batch in partitioned if by is None else tqdm(partitioned):

        for start_row in tqdm(range(len(batch))) if by is None else range(len(batch)):
            global_row_count = batch["__row_count__"][start_row]
            if global_row_count not in event_indices[event_names[0]]:
                continue

            counter += 1
            start_time = batch[start_row][time_col]
            end_time = start_time + max_span

            # TODO: replace with static
            interval = batch.filter((polars.col(time_col) >= start_time) & (polars.col(time_col) <= end_time))

            matched_sequences = {i: None for i in range(total_events)}
            matched_sequences[0] = interval[0].rename(rename_dicts[event_names[0]]).select([event_names[0] + "___row_count__"] + [event_names[0] + "_" + k for k in event_required_columns[event_names[0]]])
        
            for row in range(1, len(interval)):
                global_row_count = interval["__row_count__"][row]
                this_row_can_be = [i for i in range(1, total_events) if event_indices[event_names[i]] is None or global_row_count in event_indices[event_names[i]]]
                early_exit = False
                for seq_len in sorted(this_row_can_be)[::-1]:
                    
                    # evaluate the predicate against matched_sequences[seq_len - 1]
                    if matched_sequences[seq_len - 1] is not None and len(matched_sequences[seq_len - 1]) > 0:
                        predicate = event_predicates[seq_len]
                        assert predicate is not None
                        for col in event_independent_columns[event_names[seq_len]]:
                            predicate = predicate.replace(event_names[seq_len] + "_" + col, str(interval[col][row]))
                        # print(matched_sequences)      
                        # print("{}".format(predicate))
                        start = time.time()
                        matched = polars.SQLContext(frame=matched_sequences[seq_len - 1]).execute(
                            "select * from frame where {}".format(predicate)).collect()
                        length_dicts[event_names[seq_len]].append(len(matched_sequences[seq_len - 1]))
                        total_filter_time += time.time() - start

                        # now horizontally concatenate your table against the matched
                        if len(matched) > 0:
                            
                            matched = matched.with_columns([
                                polars.lit(interval[col][row]).alias(event_names[seq_len] + "_" + col) for col in event_required_columns[event_names[seq_len]]
                            ])
                            matched = matched.with_columns(polars.lit(interval["__row_count__"][row]).alias(event_names[seq_len] + "___row_count__"))
                            
                            if matched_sequences[seq_len] is None:
                                matched_sequences[seq_len] = matched
                            else:
                                matched_sequences[seq_len].vstack(matched, in_place=True)
                            
                            if seq_len == total_events - 1:
                                early_exit = True
                                break
                if early_exit:
                    break
            
            if matched_sequences[total_events - 1] is not None:
                if by is None:
                    end_results.append(matched_sequences[total_events - 1])
                else:
                    end_results.append(matched_sequences[total_events - 1].with_columns(polars.lit(batch[by][start_row]).alias(by)))

    print("TOTAL FILTER TIME {}".format(total_filter_time))
    for key in length_dicts:
        print(key, len(length_dicts[key]), np.mean(length_dicts[key]))
    
    print("TOTAL FILTER EVENTS: ", sum([len(length_dicts[key]) for key in length_dicts]))
    print("TOTAL FILTERED ROWS: ", sum([np.sum(length_dicts[key]) for key in length_dicts]))
    if len(end_results) > 0:
        return polars.concat(end_results)
    else:
        return None


