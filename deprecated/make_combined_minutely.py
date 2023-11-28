import glob
import polars
from tqdm import tqdm

files = glob.glob("*.parquet")
combined = []

for file in tqdm(files):
    qqq = polars.read_parquet(file).with_row_count("row_count").with_columns(polars.col("row_count").cast(polars.Int64()))
    original_qqq = qqq.with_columns(((polars.col("date").str.strptime(polars.Date).dt.timestamp('ms') // 1000) + polars.col("candle") * 60).cast(polars.UInt64()).alias("timestamp"))

    qqq = original_qqq.groupby_rolling("row_count", period = "10i", offset = "-5i").agg([
            polars.col("close").min().alias("min_close"),
            polars.col("close").max().alias("max_close"),
        ])\
        .hstack(original_qqq.select(["timestamp", "close", "symbol", "volume"]))\
        .with_columns([(polars.col("close") == polars.col("min_close")).alias("is_local_bottom"),
                    (polars.col("close") == polars.col("max_close")).alias("is_local_top")])
    combined.append(qqq)

polars.concat(combined).write_parquet("combined.parquet")
