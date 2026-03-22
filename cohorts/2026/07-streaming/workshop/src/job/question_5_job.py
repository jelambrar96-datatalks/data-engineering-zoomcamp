from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment


def create_events_source_kafka(t_env):
    table_name = "events"
    source_ddl = f"""
        CREATE TABLE {table_name} (
            PULocationID INT,
            DOLocationID INT,
            trip_distance DOUBLE,
            total_amount DOUBLE,
            lpep_pickup_datetime BIGINT,

            event_timestamp AS TO_TIMESTAMP_LTZ(lpep_pickup_datetime, 3),
            WATERMARK FOR event_timestamp AS event_timestamp - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'properties.bootstrap.servers' = 'redpanda:29092',
            'topic' = 'green-trips',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json'
        );
    """
    t_env.execute_sql(source_ddl)
    return table_name


def create_session_sink(t_env):
    table_name = "aggregation_q5"
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            PULocationID INT,
            window_start TIMESTAMP(3),
            window_end TIMESTAMP(3),
            num_trips BIGINT,
            PRIMARY KEY (PULocationID) NOT ENFORCED
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


def session_window_job():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10 * 1000)
    env.set_parallelism(1)

    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    try:
        source_table = create_events_source_kafka(t_env)
        sink_table = create_session_sink(t_env)

        # Step 1: Build session windows
        # Step 2: Count trips per session
        # Step 3: Rank sessions and keep the largest per PULocationID

        t_env.execute_sql(f"""
        INSERT INTO {sink_table}
        SELECT
            PULocationID,
            window_start,
            window_end,
            num_trips
        FROM (
            SELECT *,
                   ROW_NUMBER() OVER (
                       PARTITION BY PULocationID
                       ORDER BY num_trips DESC
                   ) as row_num
            FROM (
                SELECT
                    PULocationID,
                    window_start,
                    window_end,
                    COUNT(*) AS num_trips
                FROM TABLE(
                    SESSION(
                        TABLE {source_table},
                        DESCRIPTOR(event_timestamp),
                        INTERVAL '5' MINUTES
                    )
                )
                GROUP BY PULocationID, window_start, window_end
            )
        )
        WHERE row_num = 1
        """).wait()

    except Exception as e:
        print("Session window job failed:", str(e))


if __name__ == "__main__":
    session_window_job()
