import sqlglot
import polars
from pyjoey import nfa_cep, vector_interval_cep, nfa_interval_cep, nfa_interval_cep_c
from pyjoey.utils import verify
from technical_indicators import *


def evaluate(
    data,
    conditions,
    strategies,
    span,
    by=None,
    time_col="timestamp",
    replace_dict={},
    fix="start",
):
    """

    Look up some example use cases of this function in this file.
    Args:
        data: polars DataFrame
        conditions: list of conditions, each condition is a list of tuples of the form (event_name, predicate)
        strategies: list of tuples of the form (strategy_name, strategy)
        span: maximum span of the events
        by: column to group by
        replace_dict: dictionary of strings to replace in the predicates
    """
    for condition in conditions:
        for i in range(len(condition)):
            if condition[i][1] is not None:
                for key in replace_dict:
                    condition[i] = (
                        condition[i][0],
                        condition[i][1].replace(key, str(replace_dict[key])),
                    )

        exepcted_length = None

        unique_col = (
            (condition[0][0] + "_" + time_col)
            if fix == "start"
            else (condition[-1][0] + "_" + time_col)
        )

        for strategy_name, strategy in strategies:
            print("USING STRATEGY {}".format(strategy_name))
            if by is None:
                results = (
                    strategy(data, condition, time_col, span, by=by, fix=fix)
                    .unique([unique_col])
                    .sort(unique_col)
                )
            else:
                results = (
                    strategy(data, condition, time_col, span, by=by, fix=fix)
                    .unique([unique_col, by])
                    .sort([by, unique_col])
                )

            # make sure that all strategies return the same length at least
            if results is None:
                if exepcted_length is None:
                    exepcted_length = 0
                else:
                    assert (
                        exepcted_length == 0
                    ), "strategy {} returned None but other strategies returned {}".format(
                        strategy_name, exepcted_length
                    )
            else:
                if exepcted_length is None:
                    exepcted_length = len(results)
                else:
                    if exepcted_length != len(results):
                        print(
                            "strategy {} returned {} but other strategies returned {}".format(
                                strategy_name, len(results), exepcted_length
                            )
                        )

            print(results)
            results.write_parquet(strategy_name + ".parquet")
            # make sure that the things returned match the predicate
            verify(data, results, condition)


# Tests on Crimes dataset


def do_crimes_test():
    crimes = polars.read_parquet("data/crimes.parquet")
    results = nfa_cep(
        crimes,
        [
            ("a", "a.primary_category_id = 27"),
            (
                "b",
                """b.primary_category_id = 1 and b.LATITUDE - a.LATITUDE >= -0.025
        and b.LATITUDE - a.LATITUDE <= 0.025
        and b.LONGITUDE - a.LONGITUDE >= -0.025
        and b.LONGITUDE - a.LONGITUDE <= 0.025""",
            ),
            (
                "c",
                """c.primary_category_id = 24 and c.LATITUDE - a.LATITUDE >= -0.025
        and c.LATITUDE - a.LATITUDE <= 0.025
        and c.LONGITUDE - a.LONGITUDE >= -0.025
        and c.LONGITUDE - a.LONGITUDE <= 0.025""",
            ),
        ],
        "TIMESTAMP_LONG",
        20000000,
    )

    print(results)


def do_qqq_test():
    qqq = (
        polars.read_parquet("data/2021.parquet")
        .with_row_count("row_count")
        .with_columns(polars.col("row_count").cast(polars.Int64()))
    )
    original_qqq = qqq.with_columns(
        (
            (polars.col("date").str.strptime(polars.Date).dt.timestamp("ms") // 1000)
            + polars.col("candle") * 60
        )
        .cast(polars.UInt64())
        .alias("timestamp")
    )

    qqq = (
        original_qqq.groupby_rolling("row_count", period="10i", offset="-5i")
        .agg(
            [
                polars.col("close").min().alias("min_close"),
                polars.col("close").max().alias("max_close"),
            ]
        )
        .hstack(original_qqq.select(["timestamp", "close"]))
        .with_columns(
            [
                (polars.col("close") == polars.col("min_close")).alias(
                    "is_local_bottom"
                ),
                (polars.col("close") == polars.col("max_close")).alias("is_local_top"),
            ]
        )
    )

    conditions = [
        ascending_triangles_conditions,
        v_conditions,
        cup_and_handle_conditions,
        heads_and_shoulders_conditions,
        flag_1,
    ]
    # strategies = [("test", nfa_interval_cep_c), ("nfa_cep", nfa_cep), ("interval_vector_cep", vector_interval_cep), ("interval_nfa_cep", nfa_interval_cep)]
    strategies = [
        ("nfa_cep", nfa_cep),
        ("interval_nfa_cep", nfa_interval_cep),
        ("interval_vector_cep", vector_interval_cep),
    ]
    span = 7200
    by = None
    UPPER = 1.0025
    LOWER = 0.9975

    evaluate(
        qqq,
        conditions,
        strategies,
        span,
        by=by,
        replace_dict={"UPPER": UPPER, "LOWER": LOWER},
        fix="start",
    )
    evaluate(
        qqq,
        conditions,
        strategies,
        span,
        by=by,
        replace_dict={"UPPER": UPPER, "LOWER": LOWER},
        fix="end",
    )


def do_daily_qqq_test():
    daily_qqq = polars.read_parquet("data/daily.parquet")
    filtered_symbols = (
        daily_qqq.group_by("symbol")
        .agg([polars.count(), polars.sum("volume")])
        .filter(polars.col("count") == 252)
        .filter(polars.col("volume") > 1e8)
        .select(["symbol"])
    )
    daily_qqq = filtered_symbols.join(daily_qqq, "symbol")
    daily_qqq = daily_qqq.sort(["symbol", "date"])
    daily_qqq = polars.concat(
        [i.with_row_count("row_count") for i in daily_qqq.partition_by("symbol")]
    ).with_columns(polars.col("row_count").cast(polars.Int64()))
    daily_qqq = (
        daily_qqq.groupby_rolling(
            "row_count", period="10i", offset="-5i", by="symbol", check_sorted=False
        )
        .agg(
            [
                polars.col("close").min().alias("min_close"),
                polars.col("close").max().alias("max_close"),
            ]
        )
        .hstack(
            daily_qqq.groupby_rolling(
                "row_count", period="5i", by="symbol", check_sorted=False
            )
            .agg([polars.col("close").mean().alias("rolling_5d_mean")])
            .select(["rolling_5d_mean"])
        )
        .hstack(daily_qqq.select(["close", "high", "low"]))
        .with_columns(
            [
                (polars.col("close") == polars.col("min_close")).alias(
                    "is_local_bottom"
                ),
                (polars.col("close") == polars.col("max_close")).alias("is_local_top"),
            ]
        )
    )
    daily_qqq = daily_qqq.rename({"row_count": "timestamp"})

    conditions = [
        ascending_triangles_conditions,
        v_conditions,
        cup_and_handle_conditions,
        heads_and_shoulders_conditions,
        flag_1,
    ]
    strategies = [
        ("nfa_cep", nfa_cep),
        ("interval_vector_cep", vector_interval_cep),
        ("interval_nfa_cep", nfa_interval_cep),
    ]
    span = 60
    by = "symbol"
    UPPER = 1.01
    LOWER = 0.99

    evaluate(
        daily_qqq,
        conditions,
        strategies,
        span,
        by=by,
        replace_dict={"UPPER": UPPER, "LOWER": LOWER},
    )


def do_minutely_test():
    minutely = polars.read_parquet("data/filtered_combined.parquet")

    conditions = [
        ascending_triangles_conditions,
        cup_and_handle_conditions,
        heads_and_shoulders_conditions,
        flag_1,
    ]
    strategies = [
        ("interval_vector_cep", vector_interval_cep),
        ("interval_nfa_cep", nfa_interval_cep),
    ]
    span = 7200
    by = "symbol"
    UPPER = 1.0025
    LOWER = 0.9975

    evaluate(
        minutely,
        conditions,
        strategies,
        span,
        by=by,
        replace_dict={"UPPER": UPPER, "LOWER": LOWER},
    )


def do_mbo_test():
    mbo = polars.read_parquet("data/qqq_mbo.parquet")

    mbo = mbo.select(
        ["ts_event", "side", "price", "size", "action", "order_id"]
    ).with_row_count()
    mbo = mbo.with_columns(
        [
            (
                polars.when(polars.col("action") == "T")
                .then(polars.col("price"))
                .otherwise(None)
            )
            .forward_fill()
            .alias("last_trade_price")
        ]
    )

    layering = [
        (
            "a",
            "a.action == 'A' and a.side == 'A' and a.price > a.last_trade_price * UPPER",
        ),
        (
            "b",
            "b.action == 'A' and b.side == 'A' and b.price > b.last_trade_price * UPPER and b.size == a.size and b.price != a.price",
        ),
        (
            "c",
            "c.action == 'A' and c.side == 'A' and c.price > c.last_trade_price * UPPER and c.size == a.size and c.price != b.price and c.price != a.price",
        ),
        (
            "d",
            "d.action == 'A' and d.side == 'A' and d.price > d.last_trade_price * UPPER and d.size == a.size and d.price != a.price and d.price != b.price and d.price != c.price",
        ),
    ]

    spoofing = [
        (
            "a",
            "a.action == 'A' and a.side == 'A' and a.price > a.last_trade_price * UPPER",
        ),
        (
            "b",
            "b.action == 'A' and b.side == 'A' and b.price > b.last_trade_price * UPPER and b.price != a.price",
        ),
        (
            "c",
            "c.action == 'A' and c.side == 'A' and c.price > c.last_trade_price * UPPER and c.price != b.price and c.price != a.price",
        ),
        (
            "d",
            "d.action == 'C' and d.side == 'A' and d.price in (a.price, b.price, c.price) and d.size in (a.size, b.size, c.size)",
        ),
        (
            "e",
            "e.action == 'C' and e.side == 'A' and e.price in (a.price, b.price, c.price) and e.size in (a.size, b.size, c.size)",
        ),
        (
            "f",
            "f.action == 'C' and f.side == 'A' and f.price in (a.price, b.price, c.price) and f.size in (a.size, b.size, c.size)",
        ),
    ]

    conditions = [spoofing]

    span = 300
    by = None
    UPPER = 1.001
    LOWER = 0.999

    evaluate(
        mbo,
        conditions,
        [("interval_nfa_cep", nfa_interval_cep)],
        span,
        time_col="row_nr",
        by=by,
        replace_dict={"UPPER": UPPER, "LOWER": LOWER},
    )


def hard_test():
    data = polars.read_parquet("data/testing.parquet")
    conditions = [test_1, test_2, test_3]

    strategies = [
        ("nfa_cep", nfa_cep),
        ("interval_vector_cep", vector_interval_cep),
        ("interval_nfa_cep", nfa_interval_cep),
    ]

    evaluate(data, conditions, strategies, 6, by="ID_ZZ", time_col="date_ix")


do_qqq_test()
# do_daily_qqq_test()
# do_minutely_test()
# hard_test()
# do_mbo_test()
