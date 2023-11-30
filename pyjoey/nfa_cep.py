from .utils import * 
import sqlite3
import duckdb
import pickle
from tqdm import tqdm
import numpy as np

# fix argument is ignored in this function as it returns everything by default, so no need to specify to match start or end.
def nfa_cep(batch: polars.DataFrame, 
            events: List[Tuple], 
            time_col: str, 
            max_span:int, 
            by: Optional[Union[str, List]] = None, 
            event_udfs = {}, 
            fix = "start"):


    batch, event_names, rename_dicts, event_predicates, event_indices, event_independent_columns, event_required_columns , event_udfs, intervals, row_count_mapping = preprocess_2(batch, events, time_col, by, event_udfs, max_span)

    total_events = len(events)

    matched_events = []

    total_filter_time = 0
    total_filter_times = {event_name: [] for event_name in event_names}
    total_other_time = 0

    con = sqlite3.connect(":memory:")
    cur = con.cursor()

    # cur = duckdb.connect()

    partitioned = batch.partition_by(by) if by is not None else [batch]
    length_dicts = {event_name: [] for event_name in event_names}

    current_cols = []
    schemas = {}
    udf_return_types = {}

    for event in range(total_events - 1):
        event_name = event_names[event]
        current_cols += [event_name + "_" + k for k in event_required_columns[event_name]]
        schemas[event_names[event]] = current_cols
        cur = cur.execute("create table matched_sequences_{}({})".format(event, ", ".join(current_cols)))
        cur = cur.execute("CREATE INDEX idx_{} ON matched_sequences_{}({});".format(event, event, event_names[0] + "_" + time_col))

    # get the indices of row count cols in current_cols
    row_count_idx = [i for i in range(len(current_cols)) if "__row_count__" in current_cols[i]]

    empty = {seq_len: True for seq_len in range(0, total_events)}

    for partition in partitioned if by is None else tqdm(partitioned):

        partition_rows = partition.to_dicts()

        for seq_len in range(1, total_events):
            cur = cur.execute("delete from matched_sequences_{} ".format(seq_len - 1))

        for row in tqdm(partition_rows) if by is None else partition_rows:
            global_row_count = row["__row_count__"]
            this_row_can_be = [i for i in range(1, total_events) if event_indices[event_names[i]] is None or global_row_count in event_indices[event_names[i]]]

            current_time = row[time_col]
            
            for seq_len in sorted(this_row_can_be)[::-1]:

                if empty[seq_len - 1]:
                    continue

                cur = cur.execute("delete from matched_sequences_{} where {} < {}".format(seq_len - 1, event_names[0] + "_" + time_col, current_time - max_span))
                
                # evaluate the predicate against matched_sequences[seq_len - 1]
                predicate = event_predicates[seq_len]
                for col in event_independent_columns[event_names[seq_len]]:
                    predicate = predicate.replace(event_names[seq_len] + "_" + col, str(row[col]))
                
                start = time.time()
                
                if len(event_udfs[event_names[seq_len]]) > 0:
                    # convert matched into a polars dataframe
                    matched = cur.execute("select * from matched_sequences_{}".format(seq_len - 1)).fetchall()
                    frame = polars.DataFrame(matched, orient = "row", schema = schemas[event_names[seq_len - 1]])
                    for udf_name, wrapped_udf, current_arguments, prior_arguments in event_udfs[event_names[seq_len]]:
                        prior_col_names = [event + "_" + col for event, col in prior_arguments]
                        required_df = frame.select(prior_col_names)
                        current_arguments = {loc: row[col] for loc, col in current_arguments}
                        func = partial_any_arg(wrapped_udf, current_arguments)
                        if udf_name not in udf_return_types:
                            result = required_df.apply(lambda x: func(x))
                            if len(result) > 0:
                                udf_return_types[udf_name] = result["apply"].dtype
                        else:
                            result = required_df.apply(lambda x: func(x), return_dtype = udf_return_types[udf_name])
                        if len(result.columns) > 2:
                            raise Exception("UDF must return a single column")
                        # print(result)
                        frame = frame.with_columns(result["apply"].alias(udf_name))
                    matched = polars.SQLContext(frame=frame).execute(
                            "select * from frame where {}".format(predicate)).drop([udf_name]).collect().rows()
                
                else:
                    matched = cur.execute("select * from matched_sequences_{} where {}".format(seq_len - 1, predicate)).fetchall()

                # length_dicts[event_names[seq_len]].append(len(matched_sequences[seq_len - 1]))
                total_filter_time += time.time() - start
                total_filter_times[event_names[seq_len]].append(time.time() - start)
                
                # now horizontally concatenate your table against the matched
                if len(matched) > 0:
                    
                    if seq_len == total_events - 1:
                        for matched_row in matched:
                            row_counts = [matched_row[i] for i in row_count_idx] + [row["__row_count__"]]
                            matched_events.append(row_counts)
                        # for event in range(total_events - 1):
                        #     cur = cur.execute("delete from matched_sequences_{} where {} = {}".format(event, event_names[0] + "___row_count__", row_counts[0] ))
                    else:
                        val = tuple([row[k] for k in event_required_columns[event_names[seq_len]]])
                        matched = [row + val for row in matched]
                        # print(matched)
                        s = ",".join(["?"] * len(matched[0]))
                        cur = cur.executemany("insert into matched_sequences_{} values({})".format(seq_len, s), matched)
                        empty[seq_len] = False

            if event_indices[event_names[0]] is None or global_row_count in event_indices[event_names[0]]:
                val = ",".join([str(row[k]) for k in event_required_columns[event_names[0]]])
                cur = cur.execute("insert into matched_sequences_0 values ({})".format(val))
                empty[0] = False
        

    print("TOTAL FILTER TIME {}".format(total_filter_time))
    print("OVERHEAD", total_other_time)
    for key in length_dicts:
        print(key, len(length_dicts[key]), np.mean(length_dicts[key]))
    
    # dump total_filter_times dict as pickle
    with open("total_filter_times.pickle", "wb") as f:
        pickle.dump(total_filter_times, f)
    
    print("TOTAL FILTER EVENTS: ", sum([len(length_dicts[key]) for key in length_dicts]))
    print("TOTAL FILTERED ROWS: ", sum([np.sum(length_dicts[key]) for key in length_dicts]))

    return process_matched_events(batch, matched_events, row_count_mapping, event_names, time_col, by, total_events)


