
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

    transaction_query = """
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

    tx.run(transaction_query, **data).consume()

    # Recalculate risk scores for sender and receiver
    risk_query = """
    MATCH (account:Account)
    WHERE account.account_id IN [
        $sender_account,
        $receiver_account
    ]

    WITH account,

    size([
        (account)-[r:TRANSFERRED_TO]->()
        WHERE r.scenario_label = 'starburst_simulation'
        | r
    ]) AS starburst_sent,

    size([
        ()-[r:TRANSFERRED_TO]->(account)
        WHERE r.scenario_label = 'starburst_simulation'
        | r
    ]) AS starburst_received,

    size([
        (account)-[r:TRANSFERRED_TO]->()
        WHERE r.is_synthetic_suspicious = 1
        | r
    ]) AS suspicious_sent,

    size([
        ()-[r:TRANSFERRED_TO]->(account)
        WHERE r.is_synthetic_suspicious = 1
        | r
    ]) AS suspicious_received,

    EXISTS {
        MATCH (account)-[r:TRANSFERRED_TO]-()
        WHERE r.scenario_label = 'cycle_simulation'
    } AS participates_in_cycle

    WITH account,

    CASE
        WHEN starburst_sent > 0 THEN 20
        ELSE 0
    END
    + (starburst_received * 5)
    + CASE
        WHEN participates_in_cycle THEN 40
        ELSE 0
    END
    + ((suspicious_sent + suspicious_received) * 10)
    AS calculated_score

    WITH account, calculated_score,
         CASE
             WHEN calculated_score > 100 THEN 100
             ELSE calculated_score
         END AS final_score

    SET account.risk_score = final_score,
        account.risk_level =
            CASE
                WHEN final_score >= 70 THEN 'HIGH'
                WHEN final_score >= 40 THEN 'MEDIUM'
                ELSE 'LOW'
            END

    RETURN account.account_id AS account_id,
           account.risk_score AS risk_score,
           account.risk_level AS risk_level
    """

    result = tx.run(risk_query, **{
        "sender_account": data["sender_account"],
        "receiver_account": data["receiver_account"]
    })

    for record in result:
        print(
            "Risk updated:",
            record["account_id"],
            "Score:", record["risk_score"],
            "Level:", record["risk_level"]
        )


def main():

    driver.verify_connectivity()

    print("Neo4j connection successful!")
    print("Waiting for processed Kafka transactions...")

    try:
        for message in consumer:

            data = message.value

            # Ensure suspicious flag is always available
            data.setdefault("is_synthetic_suspicious", 0)

            try:
                with driver.session(
                    database=NEO4J_DATABASE
                ) as session:

                    session.execute_write(
                        save_transaction,
                        data
                    )

                # Commit Kafka offset only after Neo4j succeeds
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
