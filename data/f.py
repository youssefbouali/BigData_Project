# show_columns.py
# Simple script to display CSV column names and a preview (English comments)

import pandas as pd
import sys
import os

# ==================== CONFIGURATION ====================
# Change this path to your actual CSV file inside the container
CSV_PATH = "NF-CSE-CIC-IDS2018-v2_CLEAN_WITH_LABEL.csv"          # <-- CHANGE THIS !

# You can also pass the path as a command-line argument (optional but convenient)
if len(sys.argv) > 1:
    CSV_PATH = sys.argv[1]

# Check if file exists
if not os.path.exists(CSV_PATH):
    print(f"ERROR: File not found → {CSV_PATH}")
    print("\nAvailable files in common folders:")
    print("   /data/     → ", end="")
    os.system("ls -lh /data/ 2>/dev/null || echo 'empty or not mounted'")
    print("   /tmp/      → ", end="")
    os.system("ls -lh /tmp/*.csv /tmp/*.gz 2>/dev/null || echo 'nothing'")
    print("   Current folder → ", end="")
    os.system("ls -lh *.csv 2>/dev/null || echo 'nothing'")
    sys.exit(1)

print("Reading the CSV file...")
print("=" * 70)

try:
    # Read only the first 1000 rows for speed (enough to see the schema)
    df = pd.read_csv(CSV_PATH, nrows=1000, low_memory=False)

    print("File successfully loaded!")
    print(f"Number of rows in sample      : {len(df):,}")
    print(f"Number of columns          : {len(df.columns)}")
    print("=" * 70)
    print("Column names (in order):")
    print("-" * 70)

    # Print columns numbered
    for i, col in enumerate(df.columns, 1):
        print(f"{i:3d}. {col}")

    print("-" * 70)
    print("First 5 rows (preview):")
    print(df.head())

    print("\nDone! Copy the column list above and send it to me,")
    print("I will give you the final 100% working clean_and_load.py in seconds.")

except Exception as e:
    print(f"Error while reading the file: {e}")
