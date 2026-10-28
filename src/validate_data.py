
import pandas as pd
from pathlib import Path

project_dir = Path(__file__).resolve().parent.parent
file_path = project_dir / "data" / "raw" / "transactions.csv"

df = pd.read_csv(file_path)

print("\n--- Dataset shape ---")
print(df.shape)

print("\n--- First 5 records ---")
print(df.head())

print("\n--- Missing values ---")
print(df.isnull().sum())

print("\n--- Scenario counts ---")
print(df["scenario_label"].value_counts())

print("\n--- Transaction summary ---")
print(df["amount"].describe())
