
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment


TOPIC_NAME = "green-trips"
PICKUP_DATETIME_COLUMN = "lpep_pickup_datetime"


def create_events_source_kafka(t_env):
    table_name = "events"
    source_ddl = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            PULocationID INTEGER,
            DOLocationID INTEGER,
            trip_distance DOUBLE,
            passenger_count INTEGER,
            total_amount DOUBLE,
            tip_amount DOUBLE,
            {PICKUP_DATETIME_COLUMN} BIGINT,
            lpep_dropoff_datetime BIGINT
        ) WITH (
            'connector' = 'kafka',
            'properties.bootstrap.servers' = 'redpanda:29092',
            'topic' = '{TOPIC_NAME}',
            'scan.startup.mode' = 'earliest-offset',
            'properties.auto.offset.reset' = 'earliest',
            'format' = 'json'
        );
        """
    t_env.execute_sql(source_ddl)
    return table_name


def create_processed_events_sink_postgres(t_env):
    table_name = 'processed_events'
    sink_ddl = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            PULocationID INTEGER,
            DOLocationID INTEGER,
            trip_distance DOUBLE PRECISION,
            passenger_count INTEGER,
            total_amount DOUBLE PRECISION,
            tip_amount DOUBLE PRECISION,
            pickup_datetime TIMESTAMP,
            dropoff_datetime TIMESTAMP
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://postgres:5432/postgres',
            'table-name' = '{table_name}',
            'username' = 'postgres',
            'password' = 'postgres',
            'driver' = 'org.postgresql.Driver'
        );
        """
    t_env.execute_sql(sink_ddl)
    return table_name


def log_processing():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10 * 1000)  # checkpoint every 10 seconds

    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    source_table = create_events_source_kafka(t_env)
    postgres_sink = create_processed_events_sink_postgres(t_env)

    t_env.execute_sql(
        f"""
        INSERT INTO {postgres_sink}
        SELECT
            PULocationID,
            DOLocationID,
            trip_distance,
            passenger_count,
            total_amount,
            tip_amount,
            TO_TIMESTAMP_LTZ({PICKUP_DATETIME_COLUMN}, 3) as pickup_datetime,
            TO_TIMESTAMP_LTZ(lpep_dropoff_datetime, 3) as dropoff_datetime
        FROM {source_table}
        """
    ).wait()


if __name__ == '__main__':
    log_processing()
