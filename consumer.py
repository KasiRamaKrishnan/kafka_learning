from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'kasi-topic',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='earliest',
    group_id='my-group'
)

print("Starting consumer...")
for message in consumer:
    print(f"Received: {message.value}")
