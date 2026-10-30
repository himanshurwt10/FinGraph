
import os
from dotenv import load_dotenv
from neo4j import GraphDatabase

# Load environment variables
load_dotenv()

# Read Neo4j credentials
URI = os.getenv("NEO4J_URI")
USERNAME = os.getenv("NEO4J_USERNAME")
PASSWORD = os.getenv("NEO4J_PASSWORD")
DATABASE = os.getenv("NEO4J_DATABASE")

driver = GraphDatabase.driver(
    URI,
    auth=(USERNAME, PASSWORD)
)

try:
    driver.verify_connectivity()
    print("Neo4j connection successful!")

    with driver.session(database=DATABASE) as session:
        result = session.run(
            "MATCH (n) RETURN count(n) AS totalNodes"
        )
        record = result.single()

        print("Total Nodes:", record["totalNodes"])

except Exception as error:
    print("Connection failed:", error)

finally:
    driver.close()
