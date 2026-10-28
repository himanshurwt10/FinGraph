
import pandas as pd
from pathlib import Path

PROJECT_DIR = Path(__file__).resolve().parent.parent
INPUT_FILE = PROJECT_DIR / "data" / "raw" / "transactions.csv"
OUTPUT_DIR = PROJECT_DIR / "data" / "processed"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

df = pd.read_csv(INPUT_FILE)

# Analyze outgoing transactions
outgoing = (
    df.groupby("sender_account")
    .agg(
        transactions_sent=("transaction_id", "count"),
        total_sent=("amount", "sum"),
        unique_recipients=("receiver_account", "nunique")
    )
    .reset_index()
    .rename(columns={"sender_account": "account"})
)

# Analyze incoming transactions
incoming = (
    df.groupby("receiver_account")
    .agg(
        transactions_received=("transaction_id", "count"),
        total_received=("amount", "sum"),
        unique_senders=("sender_account", "nunique")
    )
    .reset_index()
    .rename(columns={"receiver_account": "account"})
)

# Combine incoming and outgoing activity
accounts = pd.merge(outgoing, incoming, on="account", how="outer")
accounts = accounts.fillna(0)

# Initial, explainable risk indicators
accounts["many_unique_senders"] = accounts["unique_senders"] >= 10
accounts["many_unique_recipients"] = accounts["unique_recipients"] >= 10
accounts["high_transaction_volume"] = (
    accounts["transactions_sent"] + accounts["transactions_received"] >= 30
)

accounts["risk_indicator_count"] = (
    accounts["many_unique_senders"].astype(int)
    + accounts["many_unique_recipients"].astype(int)
    + accounts["high_transaction_volume"].astype(int)
)

accounts["review_flag"] = accounts["risk_indicator_count"] >= 1

accounts = accounts.sort_values(
    ["risk_indicator_count", "unique_senders", "unique_recipients"],
    ascending=False
)

print("\n========== TOP ACCOUNTS FOR REVIEW ==========")
print(accounts.head(15).to_string(index=False))

output_file = OUTPUT_DIR / "account_risk_indicators.csv"
accounts.to_csv(output_file, index=False)

print(f"\nAccount analysis saved to: {output_file}")
print(f"Accounts analyzed: {len(accounts)}")
print(f"Accounts flagged for review: {accounts['review_flag'].sum()}")
print("\nNote: These are preliminary indicators, not proof of fraud.")
