
import csv
import random
from datetime import datetime, timedelta
from pathlib import Path

random.seed(42)

PROJECT_DIR = Path(__file__).resolve().parent.parent
OUTPUT_FILE = PROJECT_DIR / "data" / "raw" / "transactions.csv"

accounts = [f"A{i:04d}" for i in range(1, 121)]
transactions = []

def add_transaction(sender, receiver, amount, label="normal"):
    timestamp = datetime.now() - timedelta(
        minutes=random.randint(0, 30 * 24 * 60)
    )

    transactions.append({
        "transaction_id": f"T{len(transactions) + 1:06d}",
        "timestamp": timestamp.strftime("%Y-%m-%d %H:%M:%S"),
        "sender_account": sender,
        "receiver_account": receiver,
        "amount": round(amount, 2),
        "currency": "INR",
        "channel": random.choice(["UPI", "NEFT", "IMPS", "RTGS"]),
        "sender_country": "IN",
        "receiver_country": "IN",
        "scenario_label": label,
        "is_synthetic_suspicious": int(label != "normal")
    })

# 1. Generate ordinary transactions
for _ in range(1000):
    sender, receiver = random.sample(accounts, 2)
    add_transaction(
        sender,
        receiver,
        random.uniform(100, 50000)
    )

# 2. Simulate small transfers to a common destination
shell_account = "A9001"

for account in accounts[:25]:
    add_transaction(
        account,
        shell_account,
        9900,
        "starburst_simulation"
    )

# 3. Simulate a circular transfer pattern
add_transaction("A0101", "A0102", 15000, "cycle_simulation")
add_transaction("A0102", "A0103", 14500, "cycle_simulation")
add_transaction("A0103", "A0101", 14000, "cycle_simulation")

# Sort transactions by timestamp
transactions.sort(key=lambda row: row["timestamp"])

OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

with OUTPUT_FILE.open("w", newline="", encoding="utf-8") as file:
    writer = csv.DictWriter(
        file,
        fieldnames=transactions[0].keys()
    )
    writer.writeheader()
    writer.writerows(transactions)

print(f"Transactions generated: {len(transactions)}")
print(f"Output saved to: {OUTPUT_FILE}")
print("Synthetic suspicious scenarios: included")
