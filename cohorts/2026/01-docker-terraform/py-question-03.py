import polars as pl

df = pl.read_parquet("data/raw/green_tripdata_2025-11.parquet")


filtered_data = df.filter(
    pl.col('lpep_pickup_datetime') >= datetime.strptime('2025-11-01', '%Y-%m-%d'),
    pl.col('lpep_pickup_datetime') <= datetime.strptime('2025-12-01', '%Y-%m-%d'),
    pl.col('trip_distance') <= 1
)

count_filtered_data = filtered_data.shape[0]

print(count_filtered_data)




