import duckdb
import numpy as np
import polars
import time

n_rows_choices = [1, 10, 100, 1000, 10000, 100000, 1000000]
n_cols = 30
n_select_choices = [1, 2, 4, 8, 16]
times = {}
cur = duckdb.connect()
cur = cur.execute("PRAGMA threads=1;")
# cur.execute("DROP TABLE test")

for n_rows in n_rows_choices:
    for n_select in n_select_choices:
        cols = []
        for i in range(n_cols):
            cols.append(np.random.randint(0, 1000, n_rows))
        a = polars.from_dict({"col_" + str(i): cols[i] for i in range(n_cols)})

        col_names = ["col_" + str(i) + " integer" for i in range(n_cols)]
        cur.execute("CREATE TABLE test({})".format(",".join(col_names)))
        start = time.time()
        a = a.to_arrow()
        cur.execute("insert into test select * from a")

        runtime = time.time() - start
        selected_cols = ",".join(["col_{}".format(i) for i in range(n_select)])

        predicate = " and ".join(["col_{} > 500".format(i) for i in range(n_select)])
        start = time.time()
        for i in range(1000):
            result = cur.execute(
                "select {} from test where {}".format(selected_cols, predicate)
            ).arrow()
        runtime = time.time() - start
        print(n_rows, n_select, runtime)
        times[(n_rows, n_select)] = runtime
        cur.execute("DROP TABLE test")
