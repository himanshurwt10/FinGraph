import pandas as pd
from pathlib import Path

# Project paths
BASE_DIR = Path(__file__).resolve().parent.parent
TRANSACTIONS_FILE = BASE_DIR / "data/raw/transactions.csv"
RAW_DIR = BASE_DIR / "data/raw"

# Read existing transactions
df = pd.read_csv(TRANSACTIONS_FILE)

# Extract unique account IDs
account_ids = sorted(
    set(df["sender_account"]) | set(df["receiver_account"])
)

# Synthetic names
first_names = [
    "Aarav", "Vivaan", "Aditya", "Arjun", "Rohan",
    "Rahul", "Karan", "Ishaan", "Ananya", "Priya",
    "Neha", "Riya", "Sneha", "Kavya", "Meera"
]

last_names = [
    "Sharma", "Verma", "Gupta", "Singh", "Malhotra",
    "Kapoor", "Mehta", "Rao", "Kumar", "Agarwal"
]

# Generate synthetic persons
persons = []

for i in range(len(account_ids)):
    person_id = f"P{i + 1:04d}"

    first = first_names[i % len(first_names)]
    last = last_names[(i // len(first_names)) % len(last_names)]

    persons.append({
        "person_id": person_id,
        "person_name": f"{first} {last}"
    })

persons_df = pd.DataFrame(persons)

# Generate synthetic banks
bank_names = [
    "Apex National Bank",
    "Unity Commercial Bank",
    "Horizon Finance Bank",
    "Pioneer Trust Bank",
    "Summit Digital Bank"
]

banks = []

for i, bank_name in enumerate(bank_names):
    banks.append({
        "bank_id": f"B{i + 1:03d}",
        "bank_name": bank_name
    })

banks_df = pd.DataFrame(banks)

# Map accounts to persons and banks
accounts = []

for i, account_id in enumerate(account_ids):
    accounts.append({
        "account_id": account_id,
        "person_id": f"P{i + 1:04d}",
        "bank_id": f"B{(i % 5) + 1:03d}"
    })

accounts_df = pd.DataFrame(accounts)

# Save CSV files
persons_df.to_csv(RAW_DIR / "persons.csv", index=False)
banks_df.to_csv(RAW_DIR / "banks.csv", index=False)
accounts_df.to_csv(RAW_DIR / "accounts.csv", index=False)

print("Entity mapping generated successfully!")
print("Total persons:", len(persons_df))
print("Total banks:", len(banks_df))
print("Total accounts:", len(accounts_df))