CREATE TABLE qqq
(
    row_count bigint,
    min_close real,
    max_close real,
    timestamp bigint,
    close real,
    is_local_bottom boolean,
    is_local_top boolean
) WITH (format = 'parquet', external_location = 'file:///home/hadoop/qqq.parquet');

-- ascending_triangles_conditions =  [('a', "a.is_local_bottom"), # first bottom 
--      ('b', """b.is_local_top and b.close > a.close * 1.0025"""), # first top
--      ('c', """c.is_local_bottom and c.close < b.close * 0.9975 and c.close > a.close * 1.0025"""), # second bottom, must be higher than first bottom
--      ('d', """d.is_local_top and d.close > c.close * 1.0025 and abs(d.close / b.close) < 1.0025"""), # second top, must be similar to first top
--      ('e', """e.is_local_bottom and e.close < d.close * 0.9975 and e.close > (c.close - a.close) / (c.timestamp - a.timestamp) * (e.timestamp - a.timestamp) + a.close"""), # third bottom, didn't break support
--      ('f', """f.close > d.close * 1.0025""") #breakout resistance
-- ]


select count(*) from qqq match_recognize(
    order by timestamp
    measures A.timestamp as A_ID, B.timestamp as B_ID, C.timestamp as C_ID, D.timestamp as D_ID, E.timestamp as E_ID, F.timestamp as F_ID,
        A.close as A_close, B.close as B_close, C.close as C_close, D.close as D_close, E.close as E_close, F.close as F_close
    one row per match
    after match skip to next row
    pattern (A Z* B Z* C Z* D Z* E Z* F)
    define A as is_local_bottom, 
    Z as (timestamp - A.timestamp <= 10800),
    B as (is_local_top and close > A.close * 1.0025 and timestamp - A.timestamp <= 10800),
    C as (is_local_bottom and close < B.close * 0.9975 and close > A.close * 1.0025 and timestamp - A.timestamp <= 10800),
    D as (is_local_top and close > C.close * 1.0025 and abs(close / B.close) < 1.0025 and timestamp - A.timestamp <= 10800),
    E as (is_local_bottom and close < D.close * 0.9975 and close > (C.close - A.close) / (C.timestamp - A.timestamp) * (timestamp - A.timestamp) + A.close and timestamp - A.timestamp <= 10800),
    F as (close > D.close * 1.0025 and timestamp - A.timestamp <= 10800)
);

select count(*) from qqq match_recognize(
    order by timestamp
    measures A.timestamp as A_ID, B.timestamp as B_ID, C.timestamp as C_ID
    one row per match
    after match skip to next row
    pattern (A Z* B Z* C)
    define A as is_local_bottom, 
    Z as (timestamp - A.timestamp <= 10800),
    B as (close > A.close * 1.0025 and timestamp - A.timestamp <= 10800),
    C as (close < B.close * 0.9975 and timestamp - A.timestamp <= 10800)
);


WITH input_bucketized AS (
    SELECT *, cast(timestamp / (10800) AS bigint) AS bk
    FROM qqq
), ranges AS (
    SELECT R.bk as bk_s, M.bk as bk_e
    FROM input_bucketized AS R, input_bucketized AS M
    WHERE R.bk = M.bk AND R.is_local_bottom
        AND M.timestamp - R.timestamp <= 10800
    UNION
    SELECT R.bk as bk_s, M.bk as bk_e
    FROM input_bucketized AS R, input_bucketized AS M
    WHERE R.bk + 1 = M.bk AND R.is_local_bottom
        AND M.timestamp - R.timestamp <= 10800
), buckets AS (
    SELECT DISTINCT bk
    FROM ranges
    CROSS JOIN UNNEST(sequence(bk_s, bk_e)) AS t(bk)
), prefilter AS (
    SELECT i.* FROM input_bucketized AS i, buckets AS b
    WHERE i.bk = b.bk
) SELECT count(*) FROM prefilter match_recognize(
    order by timestamp
    measures A.timestamp as A_ID, B.timestamp as B_ID, C.timestamp as C_ID, D.timestamp as D_ID, E.timestamp as E_ID, F.timestamp as F_ID,
        A.close as A_close, B.close as B_close, C.close as C_close, D.close as D_close, E.close as E_close, F.close as F_close
    one row per match
    after match skip to next row
    pattern (A Z* B Z* C Z* D Z* E Z* F)
    define A as is_local_bottom, 
    Z as (timestamp - A.timestamp <= 10800),
    B as (is_local_top and close > A.close * 1.0025 and timestamp - A.timestamp <= 10800),
    C as (is_local_bottom and close < B.close * 0.9975 and close > A.close * 1.0025 and timestamp - A.timestamp <= 10800),
    D as (is_local_top and close > C.close * 1.0025 and abs(close / B.close) < 1.0025 and timestamp - A.timestamp <= 10800),
    E as (is_local_bottom and close < D.close * 0.9975 and close > (C.close - A.close) / (C.timestamp - A.timestamp) * (timestamp - A.timestamp) + A.close and timestamp - A.timestamp <= 10800),
    F as (close > D.close * 1.0025 and timestamp - A.timestamp <= 10800)
);

select count(*) from qqq match_recognize(
    order by timestamp
    measures A.timestamp as A_ID, B.timestamp as B_ID, C.timestamp as C_ID, D.timestamp as D_ID, E.timestamp as E_ID,
        A.close as A_close, B.close as B_close, C.close as C_close, D.close as D_close, E.close as E_close
    one row per match
    after match skip to next row
    pattern (A Z* B Z* C Z* D Z* E)
    define A as is_local_top,
    Z as (timestamp - A.timestamp <= 10800),
    B as (is_local_bottom and close < A.close * 0.9975 and timestamp - A.timestamp <= 10800),
    C as (is_local_top and close > B.close * 1.0025 and timestamp - A.timestamp <= 10800),
    D as (is_local_bottom and close < C.close * 0.9975 and close > B.close * 1.0025 and timestamp - A.timestamp <= 10800),
    E as (close > (C.close - A.close) / (C.timestamp - A.timestamp) * (timestamp - A.timestamp) + A.close and (timestamp - C.timestamp) < (C.timestamp - A.timestamp) * 0.6 and timestamp - A.timestamp <= 10800)
);

WITH input_bucketized AS (
    SELECT *, cast(timestamp / (10800) AS bigint) AS bk
    FROM qqq
), ranges AS (
    SELECT R.bk as bk_s, M.bk as bk_e
    FROM input_bucketized AS R, input_bucketized AS M
    WHERE R.bk = M.bk AND R.is_local_top
        AND M.timestamp - R.timestamp <= 10800
    UNION
    SELECT R.bk as bk_s, M.bk as bk_e
    FROM input_bucketized AS R, input_bucketized AS M
    WHERE R.bk + 1 = M.bk AND R.is_local_top
        AND M.timestamp - R.timestamp <= 10800
), buckets AS (
    SELECT DISTINCT bk
    FROM ranges
    CROSS JOIN UNNEST(sequence(bk_s, bk_e)) AS t(bk)
), prefilter AS (
    SELECT i.* FROM input_bucketized AS i, buckets AS b
    WHERE i.bk = b.bk
) SELECT count(*) FROM prefilter match_recognize(
    order by timestamp
    measures A.timestamp as A_ID, B.timestamp as B_ID, C.timestamp as C_ID, D.timestamp as D_ID, E.timestamp as E_ID,
        A.close as A_close, B.close as B_close, C.close as C_close, D.close as D_close, E.close as E_close
    one row per match
    after match skip to next row
    pattern (A Z* B Z* C Z* D Z* E)
    define A as is_local_top,
    Z as (timestamp - A.timestamp <= 10800),
    B as (is_local_bottom and close < A.close * 0.9975 and timestamp - A.timestamp <= 10800),
    C as (is_local_top and close > B.close * 1.0025 and timestamp - A.timestamp <= 10800),
    D as (is_local_bottom and close < C.close * 0.9975 and close > B.close * 1.0025 and timestamp - A.timestamp <= 10800),
    E as (close > (C.close - A.close) / (C.timestamp - A.timestamp) * (timestamp - A.timestamp) + A.close and (timestamp - C.timestamp) < (C.timestamp - A.timestamp) * 0.6 and timestamp - A.timestamp <= 10800)
);

select count(*) from qqq match_recognize(
    order by timestamp
    measures A.timestamp as A_ID, B.timestamp as B_ID, C.timestamp as C_ID, 
        A.close as A_close, B.close as B_close, C.close as C_close
    one row per match
    after match skip to next row
    pattern (A Z* B Z* C)
    define A as is_local_top,
    Z as (timestamp - A.timestamp <= 10800),
    B as (is_local_bottom and close < A.close * 0.9975 and timestamp - A.timestamp <= 10800),
    C as (close > A.close and timestamp - A.timestamp <= 10800)
);

WITH input_bucketized AS (
    SELECT *, cast(timestamp / (10800) AS bigint) AS bk
    FROM qqq
), ranges AS (
    SELECT R.bk as bk_s, M.bk as bk_e
    FROM input_bucketized AS R, input_bucketized AS M
    WHERE R.bk = M.bk AND R.is_local_top
        AND M.timestamp - R.timestamp <= 10800
    UNION
    SELECT R.bk as bk_s, M.bk as bk_e
    FROM input_bucketized AS R, input_bucketized AS M
    WHERE R.bk + 1 = M.bk AND R.is_local_top
        AND M.timestamp - R.timestamp <= 10800
), buckets AS (
    SELECT DISTINCT bk
    FROM ranges
    CROSS JOIN UNNEST(sequence(bk_s, bk_e)) AS t(bk)
), prefilter AS (
    SELECT i.* FROM input_bucketized AS i, buckets AS b
    WHERE i.bk = b.bk
) SELECT count(*) FROM prefilter match_recognize(
    order by timestamp
    measures A.timestamp as A_ID, B.timestamp as B_ID, C.timestamp as C_ID, 
        A.close as A_close, B.close as B_close, C.close as C_close
    one row per match
    after match skip to next row
    pattern (A Z* B Z* C)
    define A as is_local_top,
    Z as (timestamp - A.timestamp <= 10800),
    B as (is_local_bottom and close < A.close * 0.9975 and timestamp - A.timestamp <= 10800),
    C as (close > A.close and timestamp - A.timestamp <= 10800)
);

CREATE TABLE daily_qqq
(
    symbol varchar,
    timestamp bigint, 
    min_close real,
    max_close real,
    close real,
    is_local_bottom boolean,
    is_local_top boolean
) WITH (format = 'parquet', external_location = 'file:///home/hadoop/daily_qqq.parquet');

select count(*) from daily_qqq match_recognize(
    partition by symbol
    order by timestamp 
    measures A.timestamp as A_ID, B.timestamp as B_ID, C.timestamp as C_ID, D.timestamp as D_ID, E.timestamp as E_ID
    one row per match
    after match skip to next row
    pattern (A Z* B Z* C Z* D Z* E)
    define A as is_local_top, 
    Z as (timestamp - A.timestamp <= 30),
    B as (is_local_bottom and close < A.close * 0.99 and timestamp - A.timestamp <= 30),
    C as (is_local_top and close >= B.close * 1.01 and timestamp - A.timestamp <= 30),
    D as (is_local_bottom and close < C.close * 0.99 and close > B.close * 1.01 and timestamp - A.timestamp <= 30),
    E as (is_local_top and close > (C.close - A.close) / (C.timestamp - A.timestamp) * (timestamp - A.timestamp) + A.close  and (timestamp - C.timestamp) < (C.timestamp - A.timestamp) * 0.6 and timestamp - A.timestamp <= 30)
);

CREATE TABLE minutely
(
    row_count bigint,
    min_close real,
    max_close real,
    timestamp bigint,
    close real,
    symbol varchar,
    volume bigint,
    is_local_bottom boolean,
    is_local_top boolean
--) WITH (format = 'parquet', external_location = 'file:///home/hadoop/combined.parquet');
) WITH (format = 'parquet', external_location = 's3a://cluster-dump/combined/');

select * from minutely match_recognize(
    partition by symbol
    order by row_count
    measures A.row_count as A_ID, B.row_count as B_ID, C.row_count as C_ID, D.row_count as D_ID, E.row_count as E_ID
    one row per match
    after match skip to next row
    pattern (A Z* B Z* C Z* D Z* E)
    define A as is_local_top, 
    Z as (row_count - A.row_count <= 30),
    B as (is_local_bottom and close < A.close * 0.99 and row_count - A.row_count <= 30),
    C as (is_local_top and close >= B.close * 1.01 and row_count - A.row_count <= 30),
    D as (is_local_bottom and close < C.close * 0.99 and close > B.close * 1.01 and row_count - A.row_count <= 30),
    E as (is_local_top and close > (C.close - A.close) / (C.row_count - A.row_count) * (row_count - A.row_count) + A.close  and (row_count - C.row_count) < (C.row_count - A.row_count) * 0.6 and row_count - A.row_count <= 30)
);

select * from minutely match_recognize(
    partition by symbol
    order by timestamp 
    measures A.timestamp as A_ID, B.timestamp as B_ID, C.timestamp as C_ID, D.timestamp as D_ID, E.timestamp as E_ID
    one row per match
    after match skip to next row
    pattern (A Z* B Z* C Z* D Z* E)
    define A as is_local_top, 
    Z as (timestamp - A.timestamp <= 10800),
    B as (is_local_bottom and close < A.close * 0.99 and timestamp - A.timestamp <= 10800),
    C as (is_local_top and close >= B.close * 1.01 and timestamp - A.timestamp <= 10800),
    D as (is_local_bottom and close < C.close * 0.99 and close > B.close * 1.01 and timestamp - A.timestamp <= 10800),
    E as (is_local_top and close > (C.close - A.close) / (C.timestamp - A.timestamp) * (timestamp - A.timestamp) + A.close  and (timestamp - C.timestamp) < (C.timestamp - A.timestamp) * 0.6 and timestamp - A.timestamp <= 10800)
);