from .utils import *
import sqlite3
import pickle
from ctypes import *
import ctypes
import pyarrow as pa
import re
import sysconfig

def load_library(lib_name):
    # Get the extension for dynamic libraries
    ext = sysconfig.get_config_var('EXT_SUFFIX') or sysconfig.get_config_var('SO')

    # Construct the library name based on the platform naming conventions
    # Adjust the formatting as per your library naming convention
    formatted_lib_name = f"{lib_name}{ext}"

    # Construct the path to the library relative to the current file
    lib_path = os.path.join(os.path.dirname(__file__), formatted_lib_name)

    # Load the library
    return PyDLL(lib_path)

class StringList(ctypes.Structure):
    _fields_ = [("items", ctypes.POINTER(ctypes.c_char_p)), ("length", ctypes.c_int)]


class KeyStringListPair(ctypes.Structure):
    _fields_ = [("key", ctypes.c_char_p), ("values", StringList)]


class IntSet(ctypes.Structure):
    _fields_ = [("data", ctypes.POINTER(ctypes.c_int)), ("length", ctypes.c_int)]


class DictEntry(ctypes.Structure):
    _fields_ = [("key", ctypes.c_char_p), ("values", IntSet)]


class Vector(ctypes.Structure):
    _fields_ = [("data", ctypes.POINTER(ctypes.c_size_t)), ("size", ctypes.c_size_t)]


class Vector2D(ctypes.Structure):
    _fields_ = [("data", ctypes.POINTER(Vector)), ("size", ctypes.c_size_t)]


def pack_dict2(data):
    entries = []
    for key, values in data.items():
        if values is None:
            continue
        int_array = (ctypes.c_int * len(values))(*values)
        entries.append(DictEntry(key.encode("utf-8"), IntSet(int_array, len(values))))

    return (DictEntry * len(entries))(*entries), len(entries)


def pack_dict(data):
    num_keys = len(data)
    key_value_pairs = (KeyStringListPair * num_keys)()

    keys = []
    values_lists = []

    for index, (key, values) in enumerate(data.items()):
        # Convert key and values to ctypes-compatible format
        c_key = ctypes.c_char_p(key.encode("utf-8"))
        keys.append(c_key)  # retain a reference

        num_values = len(values)
        c_values_list = (ctypes.c_char_p * num_values)()
        for i, value in enumerate(values):
            c_values_list[i] = ctypes.c_char_p(value.encode("utf-8"))
        values_lists.append(c_values_list)  # retain a reference

        # Populate the KeyStringListPair structure
        key_value_pairs[index].key = c_key
        key_value_pairs[index].values = StringList(c_values_list, num_values)

    return key_value_pairs, num_keys

def vector_interval_cep_c(
    batch, events, time_col, max_span, by=None, event_udfs={}, fix="start"
):
    return _interval_cep_c(
        "vector", batch, events, time_col, max_span, by, event_udfs, fix
    )

def dfs_interval_cep_c(
    batch, events, time_col, max_span, by=None, event_udfs={}, fix="start"
):
    return _interval_cep_c(
        "dfs", batch, events, time_col, max_span, by, event_udfs, fix
    )

def _interval_cep_c(
    method, batch, events, time_col, max_span, by=None, event_udfs={}, fix="start"
):
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
        intervals,
        row_count_mapping,
    ) = preprocess_2(batch, events, time_col, by, event_udfs, max_span)

    total_events = len(event_names)

    event_frames = [batch.select(['__row_count__', 'timestamp', 'close']).to_arrow()]

    for i in range(1, total_events):
        local_batch = batch.select(['__row_count__', 'timestamp', 'close'])
        if event_indices[event_names[i]] == None:
            event_frames.append(local_batch.to_arrow())
        else:
            possible_col = f"__possible_{event_names[i]}__"
            event_df = polars.from_dict({"event_nrs": list(event_indices[event_names[i]])}
            ).with_columns(
                [
                    polars.col("event_nrs").cast(polars.UInt32()),
                    polars.lit(True).alias(possible_col),
                ]
            )
            frame = local_batch.join(
                event_df, left_on="__row_count__", right_on="event_nrs", how="left"
            ).filter(polars.col(possible_col)).sort("__row_count__")
            event_frames.append(frame.drop(possible_col).to_arrow())
    
    array_type = ctypes.py_object * len(event_frames)
    event_frames_c = array_type(*event_frames)

    # assert event_indices[event_names[0]] != None, "this is for things with first event filter"
    
    if method == "vector":
        lib = load_library('interval_vector')
    elif method == "dfs":
        lib = load_library('interval_dfs')
    else:
        raise NotImplementedError

    lib.MyFunction.argtypes = [
        POINTER(py_object),
        py_object,
        POINTER(KeyStringListPair),
        POINTER(KeyStringListPair),
        POINTER(c_char_p),
        c_int,
        c_char_p,
    ]
    lib.MyFunction.restype = Vector2D

    # we are going to preprocess event_predicates to replace cols from previous events with ?
    # we need to find a better way to do this too, this is way too fragile.
    # for example if the predicate is c_close < b_close * 0.999 and c_close > a_close * 1.001 on event c
    # we want to produce c_close < ? * 0.999 AND c_close > ? * 1.001 while recording b_close and a_close as independent columns IN ORDER

    event_bind_columns = {}
    event_fate_columns = {event_names[i]: set() for i in range(total_events)}
    
    for i in range(len(event_predicates)):
        if event_predicates[i] is not None:
            prior_columns = set([col.name for col in sqlglot.parse_one(event_predicates[i]).find_all(sqlglot.expressions.Column) 
                             if col.name.split("_")[0] != event_names[i]])

            column_occurences = []
            for col in prior_columns:
                event_name = col.split("_")[0]
                column_occurences.extend([(m.start(),col) for m in re.finditer(col, event_predicates[i])])
                event_fate_columns[event_name].add("_".join(col.split("_")[1:]))

            column_occurences = [i[1] for i in sorted(column_occurences)]
            event_bind_columns[event_names[i]] = column_occurences

            for col in prior_columns:
                event_predicates[i] = event_predicates[i].replace(col, "?")
            
            for col in event_independent_columns[event_names[i]]:
                event_predicates[i] = event_predicates[i].replace(event_names[i] + "_" + col, col)

        else:
            event_bind_columns[event_names[i]] = [None]

    event_fate_columns = {k: list(v) for k, v in event_fate_columns.items()}

    event_bind_columns_c, num_events = pack_dict(event_bind_columns)
    event_fate_columns_c, num_events = pack_dict(event_fate_columns)

    event_predicates_c = (ctypes.c_char_p * len(event_predicates))(
        *[
            s.encode("utf-8") if s is not None else "None".encode("utf-8")
            for s in event_predicates
        ]
    )

    data = lib.MyFunction(
        event_frames_c,
        intervals.select(["__arc__", "__crc__"]).to_arrow(),
        event_bind_columns_c,
        event_fate_columns_c,
        event_predicates_c,
        num_events,
        time_col.encode("utf-8"),
    )

    matched_events = []
    for i in range(data.size):
        vec = []
        for j in range(data.data[i].size):
            vec.append(data.data[i].data[j])
        matched_events.append(vec)

    # print(z)
    # print(time.time() - start)

    if len(matched_events) > 0:
        matched_events = [item for sublist in matched_events for item in sublist]
        matched_events = (
            batch[matched_events].select(["__row_count__", time_col, by])
            if by is not None
            else batch[matched_events].select(["__row_count__", time_col])
        )

        events = [matched_events[i::total_events] for i in range(total_events)]
        for i in range(total_events):
            if i != 0 and by is not None:
                events[i] = events[i].drop(by)
            events[i] = events[i].rename(
                {
                    "__row_count__": event_names[i] + "___row_count__",
                    time_col: event_names[i] + "_" + time_col,
                }
            )
        matched_events = polars.concat(events, how="horizontal")
        return matched_events

    else:
        return None
