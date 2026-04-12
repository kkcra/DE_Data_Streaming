import json
from kafka import KafkaConsumer

# --- CONFIG ---
BOOTSTRAP_SERVER = 'localhost:9092'
TOPIC_NAME = 'laptop_metrics'

# 1. Initialize the Consumer
consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=[BOOTSTRAP_SERVER],
    # Start from the beginning of the topic if we've never read before
    auto_offset_reset='earliest', 
    # Automatically tell the broker we've read the message
    enable_auto_commit=True,
    # The reverse of what we did in the Producer: Bytes -> JSON -> Dict
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

if __name__ == "__main__":
    print(f"🎧 Consumer started. Listening to {TOPIC_NAME}...")
    
    try:
        for message in consumer:
            # The actual data is in the .value attribute
            data = message.value
            
            # --- LOGIC: The "Brain" Part ---
            timestamp = data['timestamp']
            cpu = data['cpu_usage_pct']
            mem = data['mem_usage_pct']
            
            # Example Alerting Logic
            status = "OK"
            if cpu > 80:
                status = "🔥 HIGH LOAD"
            
            print(f"[{timestamp}] CPU: {cpu:04.1f}% | RAM: {mem:04.1f}% | Status: {status}")
            
            # --- NEXT STEP: Sink logic (Saving to DB) will go here ---
            
    except KeyboardInterrupt:
        print("Stopping Consumer...")
    finally:
        consumer.close()