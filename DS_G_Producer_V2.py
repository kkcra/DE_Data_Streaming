# First, you need the library that speaks the "Kafka protocol" (which Redpanda uses). Open your terminal/VS Code and run:
# pip install kafka-python-ng


# PowerShell
# How to verify the data is "In" the Broker
# Run this in your PowerShell to see the messages "live" inside the container:
# If you see JSON blobs scrolling by, your Producer and Broker are officially working together.
# docker exec -it redpanda-1 rpk topic consume laptop_metrics

import psutil
import time
import json
from datetime import datetime
from kafka import KafkaProducer

# --- CONFIG ---
BOOTSTRAP_SERVER = 'localhost:9092'
TOPIC_NAME = 'laptop_metrics'

# 1. Initialize the Producer
# 'value_serializer' automatically converts our dict to JSON bytes
producer = KafkaProducer(
    bootstrap_servers=[BOOTSTRAP_SERVER],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def get_metrics():
    vm = psutil.virtual_memory()
    cpu_stats = psutil.cpu_stats()
    bat = psutil.sensors_battery()
    
    return {
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "cpu_usage_pct": psutil.cpu_percent(interval=None),
        "mem_usage_pct": vm.percent,
        "ctx_switches": cpu_stats.ctx_switches,
        "battery_pct": bat.percent if bat else 100
    }

if __name__ == "__main__":
    print(f"🚀 Producer started. Sending data to {BOOTSTRAP_SERVER}...")
    
    try:
        while True:
            data = get_metrics()
            
            # 2. Send data to the broker
            producer.send(TOPIC_NAME, value=data)
            
            # 3. Flush ensures the data is actually sent before moving on
            producer.flush()
            
            print(f"Sent: {data['timestamp']} | CPU: {data['cpu_usage_pct']}%")
            time.sleep(1)
            
    except KeyboardInterrupt:
        print("Stopping Producer...")
    finally:
        producer.close()
    
