
import json
import os

from dotenv import load_dotenv
from kafka import KafkaConsumer
from neo4j import GraphDatabase

load_dotenv()


# AuraDB Configuration
AURA_URI = os.getenv("AURA_URI")
AURA_USERNAME = os.getenv("AURA_USERNAME")
AURA_PASSWORD = os.getenv("AURA_PASSWORD")
AURA_DATABASE = os.getenv("NEO4J_DATABASE", "1ba45ead")

if not all([
    AURA_URI,
    AURA_USERNAME,
    AURA_PASSWORD,
    AURA_DATABASE
]):
    raise ValueError("AuraDB configuration missing in .env")


driver = GraphDatabase.driver(
    AURA_URI,
    auth=(AURA_USERNAME, AURA_PASSWORD)
)


# Kafka Consumer Configuration
consumer = KafkaConsumer(
    "transactions_processed",
    bootstrap_servers=["localhost:9092"],
    group_id="fingraph-neo4j-sink",
    auto_offset_reset="earliest",
    enable_auto_commit=False,

    max_poll_interval_ms=1800000,
    max_poll_records=1,

    session_timeout_ms=30000,
    heartbeat_interval_ms=10000,

    value_deserializer=lambda value: json.loads(
        value.decode("utf-8")
    )
)


def save_transaction(tx, data):

    # 1. Save transaction in AuraDB
    transaction_query = """
    MERGE (sender:Account {
        account_id: $sender_account
    })

    MERGE (receiver:Account {
        account_id: $receiver_account
    })

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

    # 2. Recalculate risk scores for sender and receiver
    risk_query = """
    MATCH (account:Account)
    WHERE account.account_id IN [
        $sender_account,
        $receiver_account
    ]

    WITH account,
         account.risk_level AS previous_level,

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
             MATCH p =
                 (account)-[:TRANSFERRED_TO*2..5]->(account)

             WITH relationships(p) AS rels,
                  nodes(p) AS cycle_nodes

             WHERE
                 all(
                     n IN cycle_nodes[0..-1]
                     WHERE single(
                         m IN cycle_nodes[0..-1]
                         WHERE m = n
                     )
                 )

                 AND all(
                     i IN range(0, size(rels) - 2)
                     WHERE datetime(rels[i].timestamp)
                           <= datetime(rels[i + 1].timestamp)
                 )

                 AND duration.inSeconds(
                     datetime(rels[0].timestamp),
                     datetime(rels[-1].timestamp)
                 ).seconds <= 3600
         } AS participates_in_cycle

    WITH account,
         previous_level,

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

    WITH account,
         previous_level,

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

    RETURN
        account.account_id AS account_id,
        previous_level AS previous_level,
        account.risk_score AS risk_score,
        account.risk_level AS risk_level
    """

    result = tx.run(
        risk_query,
        sender_account=data["sender_account"],
        receiver_account=data["receiver_account"]
    )

    risk_updates = [
        dict(record) for record in result
    ]

    # 3. Generate alerts when an account newly becomes HIGH
    generated_alerts = []

    alert_query = """
    MATCH (a:Account {
        account_id: $account_id
    })

    MERGE (alert:FraudAlert {
        alert_id: $alert_id
    })

    ON CREATE SET
        alert.account_id = a.account_id,
        alert.risk_score = $risk_score,
        alert.risk_level = 'HIGH',
        alert.status = 'OPEN',
        alert.alert_type = 'HIGH_RISK_ACCOUNT',
        alert.triggering_transaction_id = $transaction_id,
        alert.created_at = datetime()

    MERGE (a)-[:GENERATED_ALERT]->(alert)

    RETURN
        alert.alert_id AS alert_id,
        alert.status AS status
    """

    for record in risk_updates:

        print(
            "Risk updated:",
            record["account_id"],
            "Score:",
            record["risk_score"],
            "Level:",
            record["risk_level"]
        )

        if (
            record["risk_level"] == "HIGH"
            and record["previous_level"] != "HIGH"
        ):

            alert_id = (
                f"ALERT-{record['account_id']}-"
                f"{data['transaction_id']}"
            )

            alert_result = tx.run(
                alert_query,
                account_id=record["account_id"],
                alert_id=alert_id,
                risk_score=record["risk_score"],
                transaction_id=data["transaction_id"]
            )

            alert_record = alert_result.single()

            if alert_record:
                generated_alerts.append({
                    "account_id": record["account_id"],
                    "alert_id": alert_record["alert_id"],
                    "risk_score": record["risk_score"],
                    "status": alert_record["status"]
                })

    return risk_updates, generated_alerts


def main():

    driver.verify_connectivity()

    print("AuraDB connection successful!")
    print("Database:", AURA_DATABASE)
    print("Waiting for processed Kafka transactions...")
    print("Temporal cycle detection enabled!")
    print("Real-time fraud alert system enabled!")

    try:

        for message in consumer:

            data = message.value

            data.setdefault(
                "is_synthetic_suspicious",
                0
            )

            try:

                with driver.session(
                    database=AURA_DATABASE
                ) as session:

                    risk_updates, generated_alerts = (
                        session.execute_write(
                            save_transaction,
                            data
                        )
                    )

                # Commit Kafka offset only after AuraDB succeeds
                consumer.commit()

                print(
                    "AuraDB updated:",
                    data.get("transaction_id")
                )

                for alert in generated_alerts:

                    print("\n🚨 HIGH RISK FRAUD ALERT 🚨")
                    print("Account:", alert["account_id"])
                    print("Alert ID:", alert["alert_id"])
                    print("Risk Score:", alert["risk_score"])
                    print("Status:", alert["status"])
                    print("-----------------------------")

            except Exception as error:

                print(
                    "Transaction processing failed:",
                    error
                )

                raise

    except KeyboardInterrupt:

        print("Stopping AuraDB sink...")

    finally:

        consumer.close()
        driver.close()


if __name__ == "__main__":
    main()
