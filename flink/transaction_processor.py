
from pathlib import Path

from pyflink.table import EnvironmentSettings, TableEnvironment


def main():
    connector_jar = (
        Path(__file__).parent
        / "connectors"
        / "flink-connector-kafka-5.0.0-2.2.jar"
    )

    kafka_client_jar = (
        Path(__file__).parent
        / "connectors"
        / "kafka-clients-4.2.0.jar"
    )

    for jar in (connector_jar, kafka_client_jar):
        if not jar.exists():
            raise FileNotFoundError(f"JAR not found: {jar}")

    settings = EnvironmentSettings.in_streaming_mode()
    table_env = TableEnvironment.create(settings)

    table_env.get_config().get_configuration().set_string(
        "pipeline.jars",
        f"{connector_jar.resolve().as_uri()};"
        f"{kafka_client_jar.resolve().as_uri()}"
    )

    # Kafka Source
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

    # Kafka Sink
    table_env.execute_sql("""
        CREATE TABLE transactions_output (
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
            'topic' = 'transactions_processed',
            'properties.bootstrap.servers' = 'localhost:9092',
            'format' = 'json'
        )
    """)

    print("Starting Flink streaming job...")
    print("Reading from Kafka: transactions")
    print("Writing to Kafka: transactions_processed")

    result = table_env.execute_sql("""
        INSERT INTO transactions_output
        SELECT
            transaction_id,
            `timestamp`,
            sender_account,
            receiver_account,
            amount,
            currency,
            channel,
            sender_country,
            receiver_country,
            scenario_label,
            is_synthetic_suspicious
        FROM transactions_source
    """)

    result.wait()


if __name__ == "__main__":
    main()

