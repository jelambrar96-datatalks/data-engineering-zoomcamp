CREATE TABLE IF NOT EXISTS green_processed_events (
    pickup_datetime TIMESTAMP,
    dropoff_datetime TIMESTAMP,
    PULocationID INTEGER,
    DOLocationID INTEGER,
    passenger_count INTEGER,
    trip_distance DOUBLE PRECISION,
    tip_amount DOUBLE PRECISION,
    total_amount DOUBLE PRECISION
)


