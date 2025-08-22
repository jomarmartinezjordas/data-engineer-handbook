import os
import logging
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings
from pyflink.table.window import Session
from pyflink.table.expressions import col, lit

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# === Configuration ===
def get_env_var(name: str, default: str = None, required: bool = False) -> str:
    value = os.environ.get(name, default)
    if required and value is None:
        raise EnvironmentError(f"Environment variable '{name}' is required but not set.")
    return value


KAFKA_URL = get_env_var("KAFKA_URL", required=True)
KAFKA_TOPIC = get_env_var("KAFKA_TOPIC", required=True)
KAFKA_GROUP = get_env_var("KAFKA_GROUP", required=True)
KAFKA_KEY = get_env_var("KAFKA_WEB_TRAFFIC_KEY", "")
KAFKA_SECRET = get_env_var("KAFKA_WEB_TRAFFIC_SECRET", "")

POSTGRES_URL = get_env_var("POSTGRES_URL", required=True)
POSTGRES_USER = get_env_var("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = get_env_var("POSTGRES_PASSWORD", "postgres")

SESSION_GAP_MINUTES = int(os.getenv("SESSION_GAP_MINUTES", "5"))

EVENT_TIMESTAMP_FORMAT = "yyyy-MM-dd''T''HH:mm:ss.SSS''Z''"


# === Table Creation Functions ===
def create_events_source_kafka(t_env: StreamTableEnvironment):
    table_name = "events"
    source_ddl = f"""
        CREATE TABLE {table_name} (
            ip VARCHAR,
            event_time VARCHAR,
            host VARCHAR,
            url VARCHAR,
            event_timestamp AS TO_TIMESTAMP(event_time, '{EVENT_TIMESTAMP_FORMAT}'),
            WATERMARK FOR event_timestamp AS event_timestamp - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{KAFKA_TOPIC}',
            'properties.bootstrap.servers' = '{KAFKA_URL}',
            'properties.group.id' = '{KAFKA_GROUP}',
            'properties.security.protocol' = 'SASL_SSL',
            'properties.sasl.mechanism' = 'PLAIN',
            'properties.sasl.jaas.config' = 'org.apache.flink.kafka.shaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="{KAFKA_KEY}" password="{KAFKA_SECRET}";',
            'scan.startup.mode' = 'latest-offset',
            'format' = 'json'
        )
    """
    logger.info(f"Creating Kafka source table: {table_name}")
    t_env.execute_sql(source_ddl)
    return table_name


def create_sessionized_events_sink_postgres(t_env: StreamTableEnvironment):
    table_name = "sessionized_events"
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            ip VARCHAR,
            host VARCHAR,
            session_start TIMESTAMP(3),
            session_end TIMESTAMP(3),
            num_events BIGINT
        ) WITH (
            'connector' = 'jdbc',
            'url' = '{POSTGRES_URL}',
            'table-name' = '{table_name}',
            'username' = '{POSTGRES_USER}',
            'password' = '{POSTGRES_PASSWORD}',
            'driver' = 'org.postgresql.Driver'
        )
    """
    logger.info(f"Creating PostgreSQL sink table: {table_name}")
    t_env.execute_sql(sink_ddl)
    return table_name


# === Main Processing Function ===
def sessionize_data():
    logger.info("Initializing Flink streaming environment...")

    # Set up streaming execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    
    # Recommended production settings
    env.set_parallelism(1)  # Adjust based on your cluster/core count
    # env.enable_checkpointing(10_000)  # Enable if needed, e.g., every 10 sec

    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    # Register time zone (optional, for timestamp consistency)
    t_env.get_config().set_local_timezone("UTC")

    # Create source and sink
    source_table = create_events_source_kafka(t_env)
    sink_table = create_sessionized_events_sink_postgres(t_env)

    # Session window aggregation
    logger.info("Building sessionization query...")
    result_table = t_env.from_path(source_table) \
        .window(Session.with_gap(lit(SESSION_GAP_MINUTES).minutes)
                 .on(col("event_timestamp"))
                 .alias("session_window")) \
        .group_by(col("session_window"), col("ip"), col("host")) \
        .select(
            col("ip"),
            col("host"),
            col("session_window").start.alias("session_start"),
            col("session_window").end.alias("session_end"),
            col("*").count.alias("num_events")  # Use col("*").count for clarity
        )

    # Insert into sink
    logger.info(f"Starting insertion into sink: {sink_table}")
    result_table.execute_insert(sink_table).wait()
    logger.info("Sessionization job completed.")


if __name__ == '__main__':
    try:
        sessionize_data()
    except Exception as e:
        logger.error("Job failed due to an error:", exc_info=True)
        raise