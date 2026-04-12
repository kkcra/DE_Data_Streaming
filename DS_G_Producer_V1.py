import psutil
import time
import json
from datetime import datetime

# --- CONFIG ---
TOPIC_NAME = "laptop_metrics"
INTERVAL = 1  # 1 second for high-frequency learning

def get_metrics():
    """Captures a snapshot of dynamic system metrics."""
    vm = psutil.virtual_memory()
    disk_io = psutil.disk_io_counters()
    cpu_stats = psutil.cpu_stats()
    bat = psutil.sensors_battery()
    
    # We create a structured dictionary (The "Payload")
    payload = {
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "cpu_usage_pct": psutil.cpu_percent(interval=None),
        "mem_usage_pct": vm.percent,
        "ctx_switches": cpu_stats.ctx_switches,
        "disk_write_bytes": disk_io.write_bytes,
        "battery_pct": bat.percent if bat else 100,
        "power_plugged": bat.power_plugged if bat else True
    }
    return payload

if __name__ == "__main__":
    print(f"Starting Producer... Sending to {TOPIC_NAME}")
    
    try:
        while True:
            data = get_metrics()
            
            # For now, we print a JSON string to simulate what goes into the Broker
            json_data = json.dumps(data)
            print(f"PRODUCER SENDING: {json_data}")
            
            # --- NEXT STEP: Kafka Producer logic goes here ---
            # producer.send(TOPIC_NAME, value=data)
            
            time.sleep(INTERVAL)
    except KeyboardInterrupt:
        print("\nStopping Producer...")