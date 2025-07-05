from kafka import KafkaProducer
import json
import time
import random

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

while True:
    data = {
        "sensor_id": "sensor-1",
        "temperature": round(random.uniform(20.0, 30.0), 2),
        "timestamp": time.time()
    }
    print(f"Sending: {data}")
    producer.send('kasi-topic', value=data)
    time.sleep(2)
