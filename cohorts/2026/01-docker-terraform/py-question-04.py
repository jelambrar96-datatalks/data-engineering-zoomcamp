import polars as pl

df = pl.read_parquet("data/raw/green_tripdata_2025-11.parquet")

longest_trip_day = (
    df.filter(pl.col('trip_distance') < 100)
    .group_by(pl.col("lpep_pickup_datetime").dt.date())
    .agg(pl.max("trip_distance"))
    .sort("trip_distance", descending=True)
    .limit(1)
)

print(longest_trip_day)

