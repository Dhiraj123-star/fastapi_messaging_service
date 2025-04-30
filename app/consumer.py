from kafka import KafkaConsumer
import json

def kafka_listener():
    consumer = KafkaConsumer(
        'test-topic',
        bootstrap_servers='kafka:9092',  # Corrected from localhost to Kafka service name
        auto_offset_reset='earliest',
        group_id='my-group',
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )

    print("Kafka consumer started... Waiting for messages.")
    for message in consumer:
        print(f"Received: {message.value}")
