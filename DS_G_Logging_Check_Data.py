# 3. How to verify your "Sink"

import sqlite3
import pandas as pd

try:
    conn = sqlite3.connect('laptop_metrics.db')
    # Using Pandas to read the data
    df = pd.read_sql_query("SELECT * FROM system_stats ORDER BY id DESC LIMIT 10", conn)
    
    if df.empty:
        print("Wait! The database exists, but it is currently empty. Is your Consumer running?")
    else:
        print("--- LATEST 10 RECORDS ---")
        print(df)
    
    conn.close()

except pd.errors.DatabaseError:
    print("❌ ERROR: The table 'system_stats' hasn't been created yet.")
    print("👉 ACTION: Run your consumer.py script first to initialize the database!")