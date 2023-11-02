# Joey

Joey is an ultra-fast embedded Python library for complex pattern recognition on time series data. Its API is based on the Pattern Query Language, a new query language that closely resembles Elastic EQL sequence. Assuming you have stock prices in a Polars DataFrame with columns `is_local_bottom`, `is_local_top`,  `timestamp` and `close`, it lets you define a pattern like this to find all ascending triangles patterns.  
~~~
ascending_triangles_conditions = [('a', "a.is_local_bottom"), # first bottom
('b', """b.is_local_top and b.close > a.close * UPPER"""), # first top
('c', """c.is_local_bottom and c.close < b.close * LOWER and c.close > a.close * UPPER"""), # second bottom, must be higher than first bottom
('d', """d.is_local_top and d.close > c.close * UPPER and abs(d.close / b.close) < UPPER"""), # second top, must be similar to first top
('e', """e.is_local_bottom and e.close < d.close * LOWER and e.close > (c.close - a.close) / (c.timestamp - a.timestamp) * (e.timestamp - a.timestamp) + a.close"""), # third bottom, didn't break support
('f', """f.close > d.close * UPPER""") #breakout resistance
]
~~~

Existing systems like SQL Match Recognize lets you do something like this, but there is no *library* that supports this functionality inside your own program. Joey fills this gap. It abides by the header-only paradigm of C++ development -- you can just take the Python functions contained in this repo, `nfa_cep`, `nfa_interval_cep` and `vector_interval_cep` and use them in your own code. They depend on some utility functions in `utils.py`. You can also package it up to be a Python library at your own leisure.

# API

The API is similar in spirit to [SQL Match Recognize](https://trino.io/docs/current/sql/match-recognize.html), Splunk [transaction](https://docs.splunk.com/Documentation/Splunk/9.1.0/SearchReference/Transaction) and Elastic EQL [sequence](https://eql.readthedocs.io/en/latest/query-guide/sequences.html). It is very simple. Let's say you have minutely OHLC data in a Polars DataFrame like this:
~~~
>>> data
shape: (96_666, 7)
┌───────────┬────────────┬────────────┬────────────┬────────────┬─────────────────┬──────────────┐
│ row_count ┆ min_close  ┆ max_close  ┆ timestamp  ┆ close      ┆ is_local_bottom ┆ is_local_top │
│ ---       ┆ ---        ┆ ---        ┆ ---        ┆ ---        ┆ ---             ┆ ---          │
│ i64       ┆ f32        ┆ f32        ┆ u64        ┆ f32        ┆ bool            ┆ bool         │
╞═══════════╪════════════╪════════════╪════════════╪════════════╪═════════════════╪══════════════╡
│ 0         ┆ 314.25     ┆ 314.720001 ┆ 1609718400 ┆ 314.670013 ┆ false           ┆ false        │
│ 1         ┆ 313.850006 ┆ 314.720001 ┆ 1609718460 ┆ 314.720001 ┆ false           ┆ true         │
│ 2         ┆ 313.820007 ┆ 314.720001 ┆ 1609718520 ┆ 314.470001 ┆ false           ┆ false        │
│ 3         ┆ 313.649994 ┆ 314.720001 ┆ 1609718580 ┆ 314.26001  ┆ false           ┆ false        │
│ …         ┆ …          ┆ …          ┆ …          ┆ …          ┆ …               ┆ …            │
~~~

We could detect all ascending triangles that happen within 7200 seconds as follows:
~~~
nfa_cep(data, ascending_triangle_conditions, "timestamp", 7200, by = None, fix = "end")
~~~

- `data` must be a Polars DataFrame. 
- `ascending_triangle_conditions` is the list of conditions listed above. 
- We then specify the timestamp column `timestamp`, which must be of integer type (Int32, Int64, UInt32, UInt64). If you have a Datetime column, you could convert it using the epoch time conversions in Polars. `data` must be presorted on this column.
- 7200 denotes the time window the pattern must occur.
- If your data contains multiple groups (e.g. stocks) and you want to find patterns that occur in each group, you can optionally provide the `by` argument. `data` must be then presorted by the timestamp column within each group.
- `fix` gives two options. `start` means we will find at least one pattern for each starting row. `end` means we will find at least one pattern for every ending row. SQL Match Recognize typically adopts `start` while real feature engineering workloads typically would prefer `end`, since you want to know whether or not a pattern has occurred with the current row as the end.

A few things to note:
1. `vector_interval_cep` and `nfa_interval_cep` have the exact same API by design. 
2. The conditions are specified by a list of tuples, which specify a list of events that must occur in sequence in the pattern. The first element of each tuple is a name of the event. The second element of the tuple is a SQL predicate following SQLite syntax. The tuple can only contain columns from current events and previous events. It **must not** contain columns from future events. You can also rewrite such dependencies by just changing the predicate of the future event. **All columns must be qualified by the table name**. Only the predicate of the first event can be None.

# Examples

Check out the included cep.py for some analysis you can do on minutely data of one symbol, daily data of different symbols, and MBO data.
