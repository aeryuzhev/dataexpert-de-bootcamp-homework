import os

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment
from pyflink.table.expressions import col, lit
from pyflink.table.window import Session


def create_processed_events_aggregated_session_sink_postgres(table_env: StreamTableEnvironment):
    table_name = 'processed_events_aggregated_session'
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            ip VARCHAR,
            host VARCHAR,
            session_start TIMESTAMP(3),
            session_end TIMESTAMP(3),
            num_hits BIGINT
        ) WITH (
            'connector' = 'jdbc',
            'url' = '{os.environ.get("POSTGRES_URL")}',
            'table-name' = '{table_name}',
            'username' = '{os.environ.get("POSTGRES_USER", "postgres")}',
            'password' = '{os.environ.get("POSTGRES_PASSWORD", "postgres")}',
            'driver' = 'org.postgresql.Driver'
        );
    """
    table_env.execute_sql(sink_ddl)
    return table_name


def create_processed_events_source_kafka(table_env: StreamTableEnvironment):
    kafka_key = os.environ.get("KAFKA_WEB_TRAFFIC_KEY", "")
    kafka_secret = os.environ.get("KAFKA_WEB_TRAFFIC_SECRET", "")
    table_name = "process_events_kafka"
    pattern = "yyyy-MM-dd''T''HH:mm:ss.SSS''Z''"
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            ip VARCHAR,
            event_time VARCHAR,
            referrer VARCHAR,
            host VARCHAR,
            url VARCHAR,
            geodata VARCHAR,
            window_timestamp AS TO_TIMESTAMP(event_time, '{pattern}'),
            WATERMARK FOR window_timestamp AS window_timestamp - INTERVAL '15' SECOND
        ) WITH (
             'connector' = 'kafka',
            'properties.bootstrap.servers' = '{os.environ.get('KAFKA_URL')}',
            'topic' = '{os.environ.get('KAFKA_TOPIC')}',
            'properties.group.id' = '{os.environ.get('KAFKA_GROUP')}',
            'properties.security.protocol' = 'SASL_SSL',
            'properties.sasl.mechanism' = 'PLAIN',
            'properties.sasl.jaas.config' = 'org.apache.flink.kafka.shaded.org.apache.kafka.common.security.plain.PlainLoginModule required username=\"{kafka_key}\" password=\"{kafka_secret}\";',  
            'scan.startup.mode' = 'latest-offset',
            'properties.auto.offset.reset' = 'latest',
            'format' = 'json'
        );
    """  # noqa
    table_env.execute_sql(sink_ddl)
    return table_name


def log_aggregation():
    # Set up the execution environment.
    exec_env = StreamExecutionEnvironment.get_execution_environment()
    exec_env.enable_checkpointing(10)
    exec_env.set_parallelism(3)

    # Set up the table environment.
    env_settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    table_env = StreamTableEnvironment.create(exec_env, environment_settings=env_settings)

    try:
        # Create Kafka source table
        kafka_source_table = create_processed_events_source_kafka(table_env)
        # Create Postgres sink table
        aggregated_sink_table = create_processed_events_aggregated_session_sink_postgres(table_env)

        (
            table_env
            .from_path(kafka_source_table)
            .window(
                Session.with_gap(lit(5).minutes)
                .on(col("window_timestamp"))
                .alias("window_session")
            )
            .group_by(
                col("window_session"),
                col("ip"),
                col("host")
            )
            .select(
                col("ip"),
                col("host"),
                col("window_session").start.alias("session_start"),
                col("window_session").end.alias("session_end"),
                col("ip").count.alias("num_hits")
            )
            .execute_insert(aggregated_sink_table)
            .wait()
        )
    except Exception as exc:
        print("Writing records from Kafka to JDBC failed: ", str(exc))


if __name__ == "__main__":
    log_aggregation()
