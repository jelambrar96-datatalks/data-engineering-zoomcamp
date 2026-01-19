import polars as pl

df = pl.read_parquet("data/raw/green_tripdata_2025-11.parquet")
df_zones = pl.read_csv("data/raw/taxi_zone_lookup.csv")

largest_tip_zone = (
    df.join(df_zones, left_on="PULocationID", right_on="LocationID")
    .filter(pl.col("Zone") == "East Harlem North")
    .sort("tip_amount", descending=True)
    .limit(1)
    .join(df_zones, left_on="DOLocationID", right_on="LocationID", suffix="_dropoff")
    .select(pl.col("Zone_dropoff"))
)

print(largest_tip_zone)
