
from pathlib import Path

from pyflink.table import EnvironmentSettings, TableEnvironment


def main():
    # Kafka connector JAR path
    jar_path = (
        Path(__file__).parent
        / "connectors"
        / "flink-connector-kafka-5.0.0-2.2.jar"
    )

    # Kafka client JAR path
    kafka_client_path = (
        Path(__file__).parent
        / "connectors"
        / "kafka-clients-4.2.0.jar"
    )

    # Check whether both JAR files exist
    if not jar_path.exists():
        raise FileNotFoundError(
            f"Kafka connector not found: {jar_path}"
        )

    if not kafka_client_path.exists():
        raise FileNotFoundError(
            f"Kafka client not found: {kafka_client_path}"
        )

    # Create streaming Table Environment
    settings = EnvironmentSettings.in_streaming_mode()
    table_env = TableEnvironment.create(settings)

    # Load both JAR files
    table_env.get_config().get_configuration().set_string(
        "pipeline.jars",
        f"{jar_path.resolve().as_uri()};"
        f"{kafka_client_path.resolve().as_uri()}"
    )

    # Create Kafka source table
    table_env.execute_sql("""
        CREATE TABLE transactions_source (
            transaction_id STRING,
            `timestamp` STRING,
            sender_account STRING,
            receiver_account STRING,
            amount DOUBLE,
            currency STRING,
            channel STRING,
            sender_country STRING,
            receiver_country STRING,
            scenario_label STRING,
            is_synthetic_suspicious INT
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'transactions',
            'properties.bootstrap.servers' = 'localhost:9092',
            'properties.group.id' = 'fingraph-flink-group',
            'scan.startup.mode' = 'latest-offset',
            'format' = 'json',
            'json.ignore-parse-errors' = 'true'
        )
    """)

    # Create print sink table
    table_env.execute_sql("""
        CREATE TABLE transactions_output (
            transaction_id STRING,
            sender_account STRING,
            receiver_account STRING,
            amount DOUBLE,
            currency STRING,
            channel STRING,
            scenario_label STRING
        ) WITH (
            'connector' = 'print'
        )
    """)

    print("Starting Flink streaming job...")
    print("Waiting for transactions from Kafka...")

    # Read transactions from Kafka and send to print sink
    result = table_env.execute_sql("""
        INSERT INTO transactions_output
        SELECT
            transaction_id,
            sender_account,
            receiver_account,
            amount,
            currency,
            channel,
            scenario_label
        FROM transactions_source
    """)

    # Keep the streaming job running
    result.wait()


if __name__ == "__main__":
    main()
