
import json
import os

from dotenv import load_dotenv
from kafka import KafkaConsumer
from neo4j import GraphDatabase

load_dotenv()

NEO4J_URI = os.getenv("NEO4J_URI")
NEO4J_USERNAME = os.getenv("NEO4J_USERNAME")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")
NEO4J_DATABASE = os.getenv("NEO4J_DATABASE")

if not all([
    NEO4J_URI,
    NEO4J_USERNAME,
    NEO4J_PASSWORD,
    NEO4J_DATABASE
]):
    raise ValueError("Neo4j configuration missing in .env")

driver = GraphDatabase.driver(
    NEO4J_URI,
    auth=(NEO4J_USERNAME, NEO4J_PASSWORD)
)

consumer = KafkaConsumer(
    "transactions_processed",
    bootstrap_servers=["localhost:9092"],
    group_id="fingraph-neo4j-sink",
    auto_offset_reset="earliest",
    enable_auto_commit=False,
    value_deserializer=lambda value: json.loads(
        value.decode("utf-8")
    )
)


def save_transaction(tx, data):
    query = """
    MERGE (sender:Account {account_id: $sender_account})
    MERGE (receiver:Account {account_id: $receiver_account})

    MERGE (sender)-[r:TRANSFERRED_TO {
        transaction_id: $transaction_id
    }]->(receiver)

    SET r.amount = $amount,
        r.currency = $currency,
        r.channel = $channel,
        r.timestamp = $timestamp,
        r.sender_country = $sender_country,
        r.receiver_country = $receiver_country,
        r.scenario_label = $scenario_label,
        r.is_synthetic_suspicious = $is_synthetic_suspicious

    RETURN r.transaction_id AS transaction_id
    """

    tx.run(query, **data).consume()


def main():
    driver.verify_connectivity()

    print("Neo4j connection successful!")
    print("Waiting for processed Kafka transactions...")

    try:
        for message in consumer:
            data = message.value

            try:
                with driver.session(
                    database=NEO4J_DATABASE
                ) as session:
                    session.execute_write(
                        save_transaction,
                        data
                    )

                consumer.commit()

                print(
                    "Neo4j updated:",
                    data.get("transaction_id")
                )

            except Exception as error:
                print("Transaction processing failed:", error)
                raise

    except KeyboardInterrupt:
        print("Stopping Neo4j sink...")

    finally:
        consumer.close()
        driver.close()


if __name__ == "__main__":
    main()
