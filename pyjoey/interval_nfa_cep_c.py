from .utils import * 
import sqlite3
import pickle
from ctypes import *
import ctypes
import pyarrow as pa
import re

class StringList(ctypes.Structure):
    _fields_ = [("items", ctypes.POINTER(ctypes.c_char_p)),
                ("length", ctypes.c_int)]

class KeyStringListPair(ctypes.Structure):
    _fields_ = [("key", ctypes.c_char_p),
                ("values", StringList)]

class IntSet(ctypes.Structure):
    _fields_ = [('data', ctypes.POINTER(ctypes.c_int)),
                ('length', ctypes.c_int)]

class DictEntry(ctypes.Structure):
    _fields_ = [('key', ctypes.c_char_p),
                ('values', IntSet)]

class Vector(ctypes.Structure):
    _fields_ = [("data", ctypes.POINTER(ctypes.c_size_t)),
                ("size", ctypes.c_size_t)]

class Vector2D(ctypes.Structure):
    _fields_ = [("data", ctypes.POINTER(Vector)),
                ("size", ctypes.c_size_t)]

def pack_dict2(data):
    entries = []
    for key, values in data.items():
        if values is None:
            continue
        int_array = (ctypes.c_int * len(values))(*values)
        entries.append(DictEntry(key.encode('utf-8'), IntSet(int_array, len(values))))

    return (DictEntry * len(entries))(*entries), len(entries)
    
def pack_dict(data):
    num_keys = len(data)
    key_value_pairs = (KeyStringListPair * num_keys)()

    keys = []
    values_lists = []

    for index, (key, values) in enumerate(data.items()):
        # Convert key and values to ctypes-compatible format
        c_key = ctypes.c_char_p(key.encode('utf-8'))
        keys.append(c_key)  # retain a reference

        num_values = len(values)
        c_values_list = (ctypes.c_char_p * num_values)()
        for i, value in enumerate(values):
            c_values_list[i] = ctypes.c_char_p(value.encode('utf-8'))
        values_lists.append(c_values_list)  # retain a reference

        # Populate the KeyStringListPair structure
        key_value_pairs[index].key = c_key
        key_value_pairs[index].values = StringList(c_values_list, num_values)

    return key_value_pairs, num_keys
    
def nfa_interval_cep_c(batch, events, time_col, max_span, by = None, event_udfs = {}):
    
    assert type(batch) == polars.DataFrame, "batch must be a polars DataFrame"
    if by is None:
        assert batch[time_col].is_sorted(), "batch must be sorted by time_col"
    else:
        assert by in batch.columns

    batch, event_names, rename_dicts, event_predicates, event_indices, event_independent_columns, event_required_columns , event_udfs, intervals, row_count_mapping = preprocess_2(batch, events, time_col, by, event_udfs, max_span)

    # assert event_indices[event_names[0]] != None, "this is for things with first event filter"
    total_events = len(event_names)
    data = batch.to_arrow()
    assert len(data) == len(batch)
    # lib = PyDLL('src/interval_nfa.cpython-37m-x86_64-linux-gpu.so')
    lib = PyDLL('src/nfa.cpython-37m-x86_64-linux-gnu.so')
    lib.MyFunction.argtypes = [py_object, py_object, POINTER(KeyStringListPair), POINTER(KeyStringListPair), POINTER(c_char_p), POINTER(DictEntry), c_int, c_int, c_char_p]
    lib.MyFunction.restype = Vector2D
    #print(lib._Z10MyFunctionP7_object(1))
    
    # we are going to preprocess event_predicates to replace cols in event_independent_columns in each event_predicate with ?, in the order in which they appear 
    
    for i in range(len(event_predicates)):
        if event_predicates[i] is not None:
            possible_cols = set(event_independent_columns[event_names[i]])
            all_col_indices = []
            all_col_names = []
            for col in possible_cols:
                cols_indices = [m.start() for m in re.finditer(event_names[i] + "_" + col, event_predicates[i])]
                all_col_indices.extend(cols_indices)
                all_col_names.extend([col] * len(cols_indices))
            
            sorted_col_names = [x for _,x in sorted(zip(all_col_indices, all_col_names))]
            for col in sorted_col_names:
                event_predicates[i] = event_predicates[i].replace(event_names[i] + "_" + col, "?")
            event_independent_columns[event_names[i]] = sorted_col_names
        else:
            event_independent_columns[event_names[i]] = []
            

    event_required_columns_c, num_events = pack_dict(event_required_columns)
    event_independent_columns_c, num_events = pack_dict(event_independent_columns)
    event_predicates_c = (ctypes.c_char_p * len(event_predicates))(*[s.encode('utf-8') if s is not None else "None".encode("utf-8") for s in event_predicates ])
    event_indices_c, num_indices = pack_dict2(event_indices)
    data = lib.MyFunction(data, intervals.select(["__arc__", "__crc__"]).to_arrow(), event_required_columns_c, event_independent_columns_c, event_predicates_c, event_indices_c, num_events, num_indices, time_col.encode('utf-8'))

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
        matched_events = batch[matched_events].select(["__row_count__", time_col, by]) if by is not None else batch[matched_events].select(["__row_count__", time_col])
        
        events = [matched_events[i::total_events] for i in range(total_events)]
        for i in range(total_events):
            if i != 0 and by is not None:
                events[i] = events[i].drop(by)
            events[i] = events[i].rename({"__row_count__" : event_names[i] + "___row_count__", time_col : event_names[i] + "_" + time_col})
        matched_events = polars.concat(events, how = 'horizontal')
        return matched_events
    
    # else:
    #     return None



