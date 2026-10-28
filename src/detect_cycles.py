
import pandas as pd

from pathlib import Path
from collections import defaultdict

PROJECT_DIR = Path(__file__).resolve().parent.parent
INPUT_FILE = PROJECT_DIR / "data" / "raw" / "transactions.csv"
OUTPUT_DIR = PROJECT_DIR / "data" / "processed"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

df = pd.read_csv(INPUT_FILE)
df["timestamp"] = pd.to_datetime(df["timestamp"])

# Build a directed graph from transactions
graph = defaultdict(set)

for _, row in df.iterrows():
    sender = row["sender_account"]
    receiver = row["receiver_account"]

    if sender != receiver:
        graph[sender].add(receiver)

# Find cycles containing exactly three distinct accounts
cycles = set()

for account_a in graph:
    for account_b in graph[account_a]:
        if account_b == account_a:
            continue

        for account_c in graph.get(account_b, set()):
            if account_c in (account_a, account_b):
                continue

            if account_a in graph.get(account_c, set()):
                # Normalize cycle rotation to avoid duplicate reports
                cycle = (account_a, account_b, account_c)
                rotations = [
                    cycle,
                    (account_b, account_c, account_a),
                    (account_c, account_a, account_b)
                ]
                cycles.add(min(rotations))

# Save results
results = []

for cycle in sorted(cycles):
    a, b, c = cycle
    results.append({
        "account_1": a,
        "account_2": b,
        "account_3": c,
        "cycle_path": f"{a} -> {b} -> {c} -> {a}"
    })

cycles_df = pd.DataFrame(
    results,
    columns=["account_1", "account_2", "account_3", "cycle_path"]
)

output_file = OUTPUT_DIR / "detected_cycles.csv"
cycles_df.to_csv(output_file, index=False)

print("\n========== CIRCULAR TRANSFER ANALYSIS ==========")
print(f"3-account cycles found: {len(cycles_df)}")
print("\nPlanted test cycle check:")

expected_cycle = {"A0101", "A0102", "A0103"}

found = any(
    {row["account_1"], row["account_2"], row["account_3"]}
    == expected_cycle
    for _, row in cycles_df.iterrows()
)

print("Expected test cycle detected:", found)

if not cycles_df.empty:
    print(cycles_df.head(20).to_string(index=False))
else:
    print("No 3-account cycles found in this dataset.")

print(f"\nResults saved to: {output_file}")
print("Reminder: a cycle is a pattern for review, not proof of fraud.")
