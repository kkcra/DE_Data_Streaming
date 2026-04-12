# ADDED: SQLITE SINK (DATABASE) FOR PERSISTENCE

import json
import sqlite3
from kafka import KafkaConsumer

# --- CONFIG ---
BOOTSTRAP_SERVER = 'localhost:9092'
TOPIC_NAME = 'laptop_metrics'
DB_NAME = 'laptop_metrics.db'

# 1. Setup SQLite Database & Table
def init_db():
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS system_stats (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp DATETIME,
            cpu_pct REAL,
            mem_pct REAL,
            ctx_switches INTEGER,
            battery_pct INTEGER
        )
    ''')
    conn.commit()
    return conn

# 2. Initialize Kafka Consumer
consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=[BOOTSTRAP_SERVER],
    auto_offset_reset='earliest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

if __name__ == "__main__":
    db_conn = init_db()
    cursor = db_conn.cursor()
    print(f"✅ Sink Connected: {DB_NAME}")
    print(f"🎧 Listening to {TOPIC_NAME}...")

    try:
        for message in consumer:
            data = message.value
            
            # 3. Insert into SQLite
            cursor.execute('''
                INSERT INTO system_stats (timestamp, cpu_pct, mem_pct, ctx_switches, battery_pct)
                VALUES (?, ?, ?, ?, ?)
            ''', (data['timestamp'], data['cpu_usage_pct'], data['mem_usage_pct'], 
                  data['ctx_switches'], data['battery_pct']))
            
            # 4. Commit to disk (The "Sink" action)
            db_conn.commit()
            
            print(f"Stored record for {data['timestamp']} | CPU: {data['cpu_usage_pct']}%")

    except KeyboardInterrupt:
        print("\nStopping Consumer...")
    finally:
        db_conn.close()
        consumer.close()