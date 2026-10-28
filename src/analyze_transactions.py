
import pandas as pd
from pathlib import Path

PROJECT_DIR = Path(__file__).resolve().parent.parent

INPUT_FILE = PROJECT_DIR / "data" / "raw" / "transactions.csv"
OUTPUT_DIR = PROJECT_DIR / "data" / "processed"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

df = pd.read_csv(INPUT_FILE)

df["timestamp"] = pd.to_datetime(df["timestamp"])
df["amount"] = pd.to_numeric(df["amount"], errors="coerce")

print("\n========== DATA OVERVIEW ==========")
print(f"Total transactions: {len(df)}")
print(f"Unique sender accounts: {df['sender_account'].nunique()}")
print(f"Unique receiver accounts: {df['receiver_account'].nunique()}")
print(f"Total transaction value: ₹{df['amount'].sum():,.2f}")

print("\n========== TRANSACTION CHANNELS ==========")
channel_summary = (
    df.groupby("channel")
    .agg(
        transaction_count=("transaction_id", "count"),
        total_amount=("amount", "sum"),
        average_amount=("amount", "mean")
    )
    .reset_index()
    .sort_values("transaction_count", ascending=False)
)

print(channel_summary.to_string(index=False))
channel_summary.to_csv(OUTPUT_DIR / "channel_summary.csv", index=False)

print("\n========== TOP RECEIVER ACCOUNTS ==========")
receiver_summary = (
    df.groupby("receiver_account")
    .agg(
        received_count=("transaction_id", "count"),
        total_received=("amount", "sum"),
        unique_senders=("sender_account", "nunique")
    )
    .reset_index()
    .sort_values(
        ["unique_senders", "total_received"],
        ascending=False
    )
)

print(receiver_summary.head(10).to_string(index=False))
receiver_summary.to_csv(
    OUTPUT_DIR / "receiver_summary.csv", index=False
)

print("\n========== HIGH-VALUE TRANSACTIONS ==========")
high_value = df[df["amount"] >= 40000].copy()
print(f"Transactions >= ₹40,000: {len(high_value)}")

high_value.to_csv(
    OUTPUT_DIR / "high_value_transactions.csv", index=False
)

print("\n========== SYNTHETIC SCENARIO SUMMARY ==========")
scenario_summary = (
    df.groupby("scenario_label")
    .agg(
        transaction_count=("transaction_id", "count"),
        total_amount=("amount", "sum")
    )
    .reset_index()
)

print(scenario_summary.to_string(index=False))
scenario_summary.to_csv(
    OUTPUT_DIR / "scenario_summary.csv", index=False
)

print("\nAnalysis complete!")
print(f"Results saved in: {OUTPUT_DIR}")