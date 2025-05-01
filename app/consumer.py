from kafka import KafkaConsumer, TopicPartition
import json
import os
import time

# Kafka consumer that resets to the earliest offset and processes all messages
# including the very first one on startup.
def kafka_listener():
    bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
    retries = int(os.getenv('KAFKA_CONSUMER_RETRIES', 5))
    delay = int(os.getenv('KAFKA_CONSUMER_RETRY_DELAY', 5))

    while retries > 0:
        try:
            consumer = KafkaConsumer(
                bootstrap_servers=bootstrap_servers,
                auto_offset_reset='earliest',
                enable_auto_commit=False,        # disable auto-commit for manual offset control
                group_id='my-group-unique',
                metadata_max_age_ms=10_000,
                value_deserializer=lambda m: safe_json_load(m)
            )

            # Subscribe and force partition assignment by polling metadata
            consumer.subscribe(['test-topic'])
            while not consumer.assignment():
                consumer.poll(timeout_ms=500)

            # Seek to the beginning of each partition
            for tp in consumer.assignment():
                consumer.seek_to_beginning(tp)

            # Initial poll to fetch any existing messages at the earliest offset
            initial_records = consumer.poll(timeout_ms=5000)
            if initial_records:
                for tp, records in initial_records.items():
                    for record in records:
                        try:
                            if isinstance(record.value, dict) and 'message' in record.value:
                                print(f"Received: {record.value['message']}")
                            else:
                                print(f"Received raw: {record.value}")
                            # Commit offset after processing
                            consumer.commit()
                        except Exception as e:
                            print(f"Error while processing initial record: {e}")

            print("Kafka consumer started... Waiting for messages.")

            # Main loop for new incoming messages
            for message in consumer:
                try:
                    if isinstance(message.value, dict) and 'message' in message.value:
                        print(f"Received: {message.value['message']}")
                    else:
                        print(f"Received raw: {message.value}")
                    consumer.commit()
                except Exception as e:
                    print(f"Error while processing message: {e}")

            break

        except Exception as e:
            print(f"Kafka consumer error: {e}. Retrying in {delay}s...")
            retries -= 1
            time.sleep(delay)

    if retries == 0:
        print("Kafka consumer failed to connect after retries.")


def safe_json_load(m):
    try:
        return json.loads(m.decode('utf-8'))
    except Exception as e:
        print(f"Deserialization error: {e}")
        return m.decode('utf-8')  # fallback raw string
