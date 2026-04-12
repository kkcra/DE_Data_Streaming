import json
import pandas as pd
from kafka import KafkaConsumer
import os
from datetime import datetime

# --- CONFIG ---
BOOTSTRAP_SERVER = 'localhost:9092'
TOPIC_NAME = 'laptop_metrics'
BATCH_SIZE = 50 
BASE_DIR = 'parquet_data'

# 1. Define the Schema (Schema Enforcement)
# This ensures that even if the producer sends a 'string' where a 'float' should be, 
# our cold storage remains consistent.
SCHEMA = {
    'timestamp': 'datetime64[ns]',
    'cpu_usage_pct': 'float64',
    'mem_usage_pct': 'float64',
    'ctx_switches': 'int64',
    'battery_pct': 'int64'
}

def get_partition_path():
    """Generates a Hive-style partition path based on current time."""
    now = datetime.now()
    path = os.path.join(
        BASE_DIR, 
        f"year={now.year}", 
        f"month={now.month:02d}", 
        f"day={now.day:02d}",
        f"hour={now.hour:02d}"
    )
    if not os.path.exists(path):
        os.makedirs(path)
    return path

def save_to_parquet(data_list):
    """Converts a list of dicts to a Columnar Parquet file with schema enforcement."""
    df = pd.DataFrame(data_list)
    
    # Apply Schema Enforcement
    for col, dtype in SCHEMA.items():
        if col in df.columns:
            df[col] = df[col].astype(dtype)
    
    # Generate unique filename using timestamp to avoid overwrites
    file_ts = datetime.now().strftime("%M%S") # Minute/Second of the batch
    target_path = get_partition_path()
    filename = os.path.join(target_path, f"batch_{file_ts}.parquet")
    
    # Save with snappy compression (standard for Parquet)
    df.to_parquet(filename, engine='pyarrow', index=False, compression='snappy')
    print(f"📦 Cold Storage: Saved {len(df)} rows to {filename}")

# --- INITIALIZE CONSUMER ---
consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=[BOOTSTRAP_SERVER],
    auto_offset_reset='earliest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

if __name__ == "__main__":
    print(f"📥 Cold Path Active. Partitioning by Hour. Batch size: {BATCH_SIZE}")
    buffer = []
    
    try:
        for message in consumer:
            buffer.append(message.value)
            
            if len(buffer) >= BATCH_SIZE:
                save_to_parquet(buffer)
                buffer = []
                
    except KeyboardInterrupt:
        if buffer:
            save_to_parquet(buffer)
        print("\nStopping Cold Path Consumer...")