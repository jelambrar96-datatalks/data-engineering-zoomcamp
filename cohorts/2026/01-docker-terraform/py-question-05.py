import polars as pl

df = pl.read_parquet("data/raw/green_tripdata_2025-11.parquet")
df_zones = pl.read_csv("data/raw/taxi_zone_lookup.csv")

largest_amount_zone = (
    df.filter(pl.col("lpep_pickup_datetime").dt.date() == pl.date(2025, 11, 18))
    .group_by("PULocationID")
    .agg(pl.sum("total_amount"))
    .join(df_zones, left_on="PULocationID", right_on="LocationID")
    .sort("total_amount", descending=True)
    .limit(1)
)

print(largest_amount_zone)
