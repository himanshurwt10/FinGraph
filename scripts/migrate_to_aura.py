
import os

from neo4j import GraphDatabase
from dotenv import load_dotenv

load_dotenv()

# Local Neo4j credentials
LOCAL_URI = os.getenv("NEO4J_URI")
LOCAL_USER = os.getenv("NEO4J_USERNAME")
LOCAL_PASSWORD = os.getenv("NEO4J_PASSWORD")

# Neo4j AuraDB credentials
AURA_URI = os.getenv("AURA_URI")
AURA_USER = os.getenv("AURA_USERNAME")
AURA_PASSWORD = os.getenv("AURA_PASSWORD")

BATCH_SIZE = 500


def check_config():
    values = [
        LOCAL_URI,
        LOCAL_USER,
        LOCAL_PASSWORD,
        AURA_URI,
        AURA_USER,
        AURA_PASSWORD
    ]

    if not all(values):
        raise ValueError("Missing database credentials in .env")

    if LOCAL_URI == AURA_URI:
        raise ValueError("Local and AuraDB URIs must be different")


def fetch_all(driver, query):
    with driver.session() as session:
        return list(session.run(query))


def migrate_nodes(source, target):
    queries = {
        "Account": """
            UNWIND $rows AS row
            MERGE (n:Account {account_id: row.account_id})
            SET n += row.props
        """,
        "Person": """
            UNWIND $rows AS row
            MERGE (n:Person {person_id: row.person_id})
            SET n += row.props
        """,
        "Bank": """
            UNWIND $rows AS row
            MERGE (n:Bank {bank_id: row.bank_id})
            SET n += row.props
        """,
        "FraudAlert": """
            UNWIND $rows AS row
            MERGE (n:FraudAlert {alert_id: row.alert_id})
            SET n += row.props
        """
    }

    key_fields = {
        "Account": "account_id",
        "Person": "person_id",
        "Bank": "bank_id",
        "FraudAlert": "alert_id"
    }

    total = 0

    for label, query in queries.items():
        records = fetch_all(
            source,
            f"MATCH (n:{label}) RETURN properties(n) AS props"
        )

        rows = []
        key_field = key_fields[label]

        for record in records:
            props = dict(record["props"])

            rows.append({
                key_field: props[key_field],
                "props": props
            })

        for i in range(0, len(rows), BATCH_SIZE):
            batch = rows[i:i + BATCH_SIZE]

            with target.session() as session:
                session.run(query, rows=batch).consume()

        print(f"{label}: {len(rows)} nodes migrated")
        total += len(rows)

    return total


def migrate_relationships(source, target):
    queries = {
        "OWNS": """
            UNWIND $rows AS row
            MATCH (p:Person {person_id: row.start_id})
            MATCH (a:Account {account_id: row.end_id})
            MERGE (p)-[:OWNS]->(a)
        """,

        "HELD_AT": """
            UNWIND $rows AS row
            MATCH (a:Account {account_id: row.start_id})
            MATCH (b:Bank {bank_id: row.end_id})
            MERGE (a)-[:HELD_AT]->(b)
        """,

        "GENERATED_ALERT": """
            UNWIND $rows AS row
            MATCH (a:Account {account_id: row.start_id})
            MATCH (f:FraudAlert {alert_id: row.end_id})
            MERGE (a)-[:GENERATED_ALERT]->(f)
        """,

        "TRANSFERRED_TO": """
            UNWIND $rows AS row
            MATCH (a:Account {account_id: row.start_id})
            MATCH (b:Account {account_id: row.end_id})
            MERGE (a)-[r:TRANSFERRED_TO {
                transaction_id: row.props.transaction_id
            }]->(b)
            SET r += row.props
        """
    }

    total = 0

    for rel_type, query in queries.items():
        records = fetch_all(
            source,
            f"""
            MATCH (a)-[r:{rel_type}]->(b)
            RETURN properties(r) AS props,
                   properties(a) AS start_props,
                   properties(b) AS end_props
            """
        )

        rows = []

        for record in records:
            start_props = dict(record["start_props"])
            end_props = dict(record["end_props"])

            if rel_type == "OWNS":
                start_id = start_props["person_id"]
                end_id = end_props["account_id"]

            elif rel_type == "HELD_AT":
                start_id = start_props["account_id"]
                end_id = end_props["bank_id"]

            elif rel_type == "GENERATED_ALERT":
                start_id = start_props["account_id"]
                end_id = end_props["alert_id"]

            else:
                start_id = start_props["account_id"]
                end_id = end_props["account_id"]

            rows.append({
                "start_id": start_id,
                "end_id": end_id,
                "props": dict(record["props"])
            })

        for i in range(0, len(rows), BATCH_SIZE):
            batch = rows[i:i + BATCH_SIZE]

            with target.session() as session:
                session.run(query, rows=batch).consume()

        print(f"{rel_type}: {len(rows)} relationships migrated")
        total += len(rows)

    return total


def main():
    check_config()

    source = GraphDatabase.driver(
        LOCAL_URI,
        auth=(LOCAL_USER, LOCAL_PASSWORD)
    )

    target = GraphDatabase.driver(
        AURA_URI,
        auth=(AURA_USER, AURA_PASSWORD)
    )

    try:
        print("Connecting to local Neo4j...")
        source.verify_connectivity()

        print("Connecting to AuraDB...")
        target.verify_connectivity()

        print("\nBoth databases connected successfully.")

        # Check AuraDB before migration
        with target.session() as session:
            node_count = session.run(
                "MATCH (n) RETURN count(n) AS total"
            ).single()["total"]

            relationship_count = session.run(
                "MATCH ()-[r]->() RETURN count(r) AS total"
            ).single()["total"]

        print(f"\nCurrent AuraDB nodes: {node_count}")
        print(f"Current AuraDB relationships: {relationship_count}")

        if node_count > 0 or relationship_count > 0:
            raise RuntimeError(
                "AuraDB is not empty. Migration stopped to avoid "
                "mixing existing data. Check the database first."
            )

        print("\nStarting node migration...")
        node_total = migrate_nodes(source, target)

        print("\nStarting relationship migration...")
        relationship_total = migrate_relationships(source, target)

        print("\nMigration completed successfully!")
        print("Nodes migrated:", node_total)
        print("Relationships migrated:", relationship_total)

    finally:
        source.close()
        target.close()


if __name__ == "__main__":
    main()
