from utils import * 

def nfa_cep(batch, events, time_col, max_span, by = None):
    
    assert type(batch) == polars.DataFrame, "batch must be a polars DataFrame"
    if by is None:
        assert batch[time_col].is_sorted(), "batch must be sorted by time_col"
    else:
        assert by in batch.columns

    batch, event_names, rename_dicts, event_predicates, event_indices, event_independent_columns, event_required_columns = preprocess_2(batch, events, time_col, by)

    total_events = len(events)
    total_filter_time = 0
    total_other_time = 0

    partitioned = batch.partition_by(by) if by is not None else [batch]

    results = []

    for batch in tqdm(partitioned) if by is not None else partitioned:

        assert batch[time_col].is_sorted()

        matched_sequences = {i: None for i in range(total_events)}
        matched_sequences[0] = batch[0].rename(rename_dicts[event_names[0]]).select([event_names[0] + "_" + k for k in event_required_columns[event_names[0]]])

        for row in tqdm(range(len(batch))) if by is None else range(len(batch)):
            current_time = batch[time_col][row]
            global_row_count = batch["__row_count__"][row]
            this_row_can_be = [i for i in range(1, total_events) if event_indices[event_names[i]] is None or global_row_count in event_indices[event_names[i]]]
                
            # if row % 1000 == 0:
            #     print(total_filter_time, total_other_time)
            
            for seq_len in this_row_can_be:
                
                # evaluate the predicate against matched_sequences[seq_len - 1]
                if matched_sequences[seq_len - 1] is not None and len(matched_sequences[seq_len - 1]) > 0:

                    predicate = event_predicates[seq_len]
                    assert predicate is not None
                    for col in event_independent_columns[event_names[seq_len]]:
                        predicate = predicate.replace(event_names[seq_len] + "_" + col, str(batch[col][row]))

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
                if global_row_count in event_indices[event_names[0]]:
                    matched_sequences[0].vstack(batch[row].rename(rename_dicts[event_names[0]]).select([event_names[0] + "_" + k for k in event_required_columns[event_names[0]]]), in_place=True)
        
        if matched_sequences[total_events - 1] is not None:
            if by is not None:
                key = batch[by][0]
                results.append(matched_sequences[total_events - 1].filter(polars.col(event_names[-1] + "_" + time_col) - polars.col(event_names[0] + "_" + time_col) <= max_span).with_columns(polars.lit(key).alias(by)))
            else:
                results.append(matched_sequences[total_events - 1].filter(polars.col(event_names[-1] + "_" + time_col) - polars.col(event_names[0] + "_" + time_col) <= max_span))
    
    return polars.concat(results)