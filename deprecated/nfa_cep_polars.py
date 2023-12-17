from utils import *
from functools import partial
import numpy as np


def nfa_cep_polars(batch, events, time_col, max_span, by=None, udfs={}):
    assert type(batch) == polars.DataFrame, "batch must be a polars DataFrame"
    if by is None:
        assert batch[time_col].is_sorted(), "batch must be sorted by time_col"
    else:
        assert by in batch.columns

    (
        batch,
        event_names,
        rename_dicts,
        event_predicates,
        event_indices,
        event_independent_columns,
        event_required_columns,
        event_udfs,
    ) = preprocess_2(batch, events, time_col, by, udfs, max_span)

    total_events = len(events)
    total_filter_time = 0
    total_match_time = 0
    total_other_time = 0

    partitioned = batch.partition_by(by) if by is not None else [batch]

    results = []
    udf_return_types = {}
    length_dicts = {event_name: [] for event_name in event_names}

    for batch in tqdm(partitioned) if by is not None else partitioned:
        assert batch[time_col].is_sorted()

        time_col_npy = batch[time_col].to_numpy()
        row_count_npy = batch["__row_count__"].to_numpy()

        matched_sequences = {i: None for i in range(total_events)}

        for row in tqdm(range(len(batch))) if by is None else range(len(batch)):
            start = time.time()
            current_time = time_col_npy[row]
            global_row_count = row_count_npy[row]
            this_row_can_be = [
                i
                for i in range(1, total_events)
                if event_indices[event_names[i]] is None
                or global_row_count in event_indices[event_names[i]]
            ]
            total_other_time += time.time() - start

            # we should do this in reverse order because once a row is matched on event B it should not match against itself for C.
            for seq_len in sorted(this_row_can_be)[::-1]:
                # evaluate the predicate against matched_sequences[seq_len - 1]
                if (
                    matched_sequences[seq_len - 1] is not None
                    and len(matched_sequences[seq_len - 1]) > 0
                ):
                    predicate = event_predicates[seq_len]
                    assert predicate is not None
                    for col in event_independent_columns[event_names[seq_len]]:
                        predicate = predicate.replace(
                            event_names[seq_len] + "_" + col, str(batch[col][row])
                        )

                    # print(matched_sequences)
                    start = time.time()

                    # filtering by the first event is very slow for some reason likely because of discontiguous memory
                    first_event_time_col = event_names[0] + "_" + time_col
                    index = np.searchsorted(
                        matched_sequences[seq_len - 1][first_event_time_col].to_numpy(),
                        current_time - max_span,
                        side="left",
                    )
                    matched_sequences[seq_len - 1] = matched_sequences[seq_len - 1][
                        index:
                    ]
                    # this is slow, use the numpy impl
                    # matched_sequences[seq_len - 1] = matched_sequences[seq_len - 1].set_sorted(first_event_time_col).filter(polars.col(first_event_time_col) >= current_time - max_span)

                    total_filter_time += time.time() - start

                    # print("{}".format(predicate))

                    # apply your UDFs here
                    start = time.time()

                    frame = matched_sequences[seq_len - 1]
                    for (
                        udf_name,
                        wrapped_udf,
                        current_arguments,
                        prior_arguments,
                    ) in event_udfs[event_names[seq_len]]:
                        prior_col_names = [
                            event + "_" + col for event, col in prior_arguments
                        ]
                        required_df = matched_sequences[seq_len - 1].select(
                            prior_col_names
                        )
                        current_arguments = {
                            loc: batch[col][row] for loc, col in current_arguments
                        }
                        func = partial_any_arg(wrapped_udf, current_arguments)
                        if udf_name not in udf_return_types:
                            result = required_df.apply(lambda x: func(x))
                            if len(result) > 0:
                                udf_return_types[udf_name] = result["apply"].dtype
                        else:
                            result = required_df.apply(
                                lambda x: func(x),
                                return_dtype=udf_return_types[udf_name],
                            )
                        if len(result.columns) > 2:
                            raise Exception("UDF must return a single column")
                        # print(result)
                        frame = matched_sequences[seq_len - 1].with_columns(
                            result["apply"].alias(udf_name)
                        )

                    start = time.time()
                    matched = (
                        polars.SQLContext(frame=frame)
                        .execute("select * from frame where {}".format(predicate))
                        .collect()
                    )
                    total_match_time += time.time() - start
                    start = time.time()
                    # print("select * from frame where {}".format(predicate), "LEN:{}".format(len(frame)), time.time() - start)
                    length_dicts[event_names[seq_len]].append(len(frame))
                    # now horizontally concatenate your table against the matched, with_columns with polars lit is faster than my repeat_rows function
                    if len(matched) > 0:
                        matched = matched.with_columns(
                            [
                                polars.lit(batch[col][row]).alias(
                                    event_names[seq_len] + "_" + col
                                )
                                for col in event_required_columns[event_names[seq_len]]
                            ]
                        )
                        matched = matched.with_columns(
                            polars.lit(batch["__row_count__"][row]).alias(
                                event_names[seq_len] + "___row_count__"
                            )
                        )

                        if matched_sequences[seq_len] is None:
                            matched_sequences[seq_len] = matched
                        else:
                            matched_sequences[seq_len] = (
                                matched_sequences[seq_len]
                                .vstack(matched)
                                .sort(event_names[0] + "_" + time_col)
                            )
                    total_other_time += time.time() - start

            if (
                event_indices[event_names[0]] is None
                or global_row_count in event_indices[event_names[0]]
            ):
                if matched_sequences[0] is None:
                    matched_sequences[0] = (
                        batch[row]
                        .rename(rename_dicts[event_names[0]])
                        .select(
                            [event_names[0] + "___row_count__"]
                            + [
                                event_names[0] + "_" + k
                                for k in event_required_columns[event_names[0]]
                            ]
                        )
                    )
                else:
                    matched_sequences[0].vstack(
                        batch[row]
                        .rename(rename_dicts[event_names[0]])
                        .select(
                            [event_names[0] + "___row_count__"]
                            + [
                                event_names[0] + "_" + k
                                for k in event_required_columns[event_names[0]]
                            ]
                        ),
                        in_place=True,
                    )

        if matched_sequences[total_events - 1] is not None:
            if by is not None:
                key = batch[by][0]
                results.append(
                    matched_sequences[total_events - 1].with_columns(
                        polars.lit(key).alias(by)
                    )
                )
            else:
                results.append(matched_sequences[total_events - 1])

    print(total_filter_time, total_match_time, total_other_time)
    print("TIME SPENT IN FILTERING {}".format(total_match_time))
    for key in length_dicts:
        print(key, len(length_dicts[key]), np.mean(length_dicts[key]))

    print(
        "TOTAL FILTER EVENTS: ", sum([len(length_dicts[key]) for key in length_dicts])
    )
    print(
        "TOTAL FILTERED ROWS: ",
        sum([np.sum(length_dicts[key]) for key in length_dicts]),
    )
    if len(results) > 0:
        return polars.concat(results)
    else:
        return None
