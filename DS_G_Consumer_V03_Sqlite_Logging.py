# ADDED ALERTING (LOGGING) & THRESHOLD CHECKS
# ADD CLEANUP LOGIC TO DELETE OLD ENTRIES FROM SQLITE

import json
import sqlite3
import logging
from kafka import KafkaConsumer

# --- CONFIG ---
BOOTSTRAP_SERVER = 'localhost:9092'
TOPIC_NAME = 'laptop_metrics'
DB_NAME = 'laptop_metrics.db'

# Thresholds for Alerting
CPU_THRESHOLD = 90.0
MEM_THRESHOLD = 85.0

# 1. Setup Alert Logger (The "Ticketing" system)
logging.basicConfig(
    filename='system_alerts.log', 
    level=logging.WARNING, 
    format='%(asctime)s - ALERT: %(message)s'
)

# 2. Setup SQLite Database & Table
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

# 3. Initialize Kafka Consumer
consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=[BOOTSTRAP_SERVER],
    auto_offset_reset='earliest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

if __name__ == "__main__":
    db_conn = init_db()
    cursor = db_conn.cursor()
    msg_count = 0  # Add a counter here

    print(f"✅ Sink Connected: {DB_NAME}")
    print(f"✅ Alerting Active (CPU > {CPU_THRESHOLD}%, RAM > {MEM_THRESHOLD}%)")
    print(f"🎧 Listening to {TOPIC_NAME}...")

    try:
        for message in consumer:
            data = message.value
            msg_count += 1
            
            # --- BRAIN: THRESHOLD LOGIC ---
            cpu = data['cpu_usage_pct']
            mem = data['mem_usage_pct']

            if cpu > CPU_THRESHOLD:
                alert_msg = f"CPU High: {cpu}%"
                print(f"⚠️  {alert_msg}")
                logging.warning(alert_msg)

            if mem > MEM_THRESHOLD:
                alert_msg = f"Memory High: {mem}%"
                print(f"⚠️  {alert_msg}")
                logging.warning(alert_msg)
            
            # --- SINK: INSERT INTO SQLITE ---
            cursor.execute('''
                INSERT INTO system_stats (timestamp, cpu_pct, mem_pct, ctx_switches, battery_pct)
                VALUES (?, ?, ?, ?, ?)
            ''', (data['timestamp'], data['cpu_usage_pct'], data['mem_usage_pct'], 
                  data['ctx_switches'], data['battery_pct']))

            # --- CLEANUP DATABASE - Optimized ---
            if msg_count % 100 == 0:
                # Use 'localtime' to ensure it matches the datetime.now() from your producer
                cursor.execute('''
                    DELETE FROM system_stats 
                    WHERE timestamp < datetime('now', 'localtime', '-1 hour')
                ''')
                db_conn.commit() # Save both the inserts and the deletions at once
                print(f"🧹 Database Cleanup: Removed logs older than 1 hour.")
            else:
                # Regular commit for every message (or every 10 for more speed)
                db_conn.commit()

            # Just a heartbeat print
            print(f"Processed: {data['timestamp']} | CPU: {cpu}%")

    except KeyboardInterrupt:
        print("\nStopping Consumer...")
        # FINAL POLISH: Commit any remaining records in the current batch before exiting
        db_conn.commit()
    finally:
        db_conn.close()
        consumer.close()
        print("🔌 Connections closed safely.")