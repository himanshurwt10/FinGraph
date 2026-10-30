
import json
import random
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer
from kafka.errors import KafkaError

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "transactions"

ACCOUNTS = [f"A{i:04d}" for i in range(1, 121)]
CHANNELS = ["UPI", "NEFT", "IMPS", "RTGS", "CARD"]
COUNTRIES = ["IN", "US", "GB", "SG"]

def create_transaction():
    sender, receiver = random.sample(ACCOUNTS, 2)

    return {
        "transaction_id": str(uuid.uuid4()),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "sender_account": sender,
        "receiver_account": receiver,
        "amount": round(random.uniform(100, 50000), 2),
        "currency": "INR",
        "channel": random.choice(CHANNELS),
        "sender_country": "IN",
        "receiver_country": random.choice(COUNTRIES),
        "scenario_label": "streaming_simulation",
        "is_synthetic_suspicious": 0
    }

def main():
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        acks="all",
        retries=5
    )

    print(f"Sending synthetic events to Kafka topic: {TOPIC}")
    print("Press Ctrl+C to stop.")

    count = 0

    try:
        while True:
            transaction = create_transaction()

            try:
                producer.send(TOPIC, value=transaction).get(timeout=10)
                count += 1
                print(f"Published #{count}: {transaction}")
            except KafkaError as error:
                print(f"Kafka publish failed: {error}")

            time.sleep(2)

    except KeyboardInterrupt:
        print(f"\nStopped. Total published in this run: {count}")

    finally:
        producer.flush()
        producer.close()

if __name__ == "__main__":
    main()
