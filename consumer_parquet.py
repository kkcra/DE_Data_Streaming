import json
import pandas as pd
from kafka import KafkaConsumer
import os

# --- CONFIG ---
BOOTSTRAP_SERVER = 'localhost:9092'
TOPIC_NAME = 'laptop_metrics'
BATCH_SIZE = 50  # We wait for 50 records before writing a file
OUTPUT_DIR = 'parquet_data'

# Ensure the output directory exists
if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)

# Initialize Consumer
consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=[BOOTSTRAP_SERVER],
    auto_offset_reset='earliest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

def save_to_parquet(data_list, batch_number):
    """Converts a list of dicts to a Parquet file."""
    df = pd.DataFrame(data_list)
    filename = f"{OUTPUT_DIR}/metrics_batch_{batch_number}.parquet"
    df.to_parquet(filename, engine='pyarrow', index=False)
    print(f"💾 Saved Batch {batch_number} to {filename}")

if __name__ == "__main__":
    print(f"📥 Parquet Consumer Active. Batch Size: {BATCH_SIZE}")
    
    buffer = []
    batch_count = 0
    
    try:
        for message in consumer:
            buffer.append(message.value)
            
            # Check if we hit the limit
            if len(buffer) >= BATCH_SIZE:
                batch_count += 1
                save_to_parquet(buffer, batch_count)
                buffer = []  # Reset the buffer
                
    except KeyboardInterrupt:
        # If we stop, save whatever is left in the buffer so we don't lose it
        if buffer:
            save_to_parquet(buffer, "final")
        print("\nStopping Parquet Consumer...")