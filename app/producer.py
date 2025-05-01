# producers.py
from kafka import KafkaProducer
import json
import os
from pika import BlockingConnection, ConnectionParameters, PlainCredentials
from urllib.parse import urlparse

# Kafka producer
def send_message_to_kafka(message: str):
    bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    producer.send('test-topic', {'message': message})
    producer.flush()
    print(f"Message sent to Kafka: {message}")
    producer.close()

# RabbitMQ producer
def send_message_to_rabbitmq(message: str):
    rabbit_url = os.getenv('RABBITMQ_URL', 'amqp://guest:guest@rabbitmq:5672/')
    parsed = urlparse(rabbit_url)
    credentials = PlainCredentials(parsed.username, parsed.password)
    connection = BlockingConnection(ConnectionParameters(
        host=parsed.hostname,
        port=parsed.port,
        virtual_host=parsed.path[1:] if parsed.path else '/',
        credentials=credentials
    ))
    channel = connection.channel()
    channel.queue_declare(queue='test-queue')
    channel.basic_publish(exchange='',
                          routing_key='test-queue',
                          body=json.dumps({"message": message}))
    print(f"Message sent to RabbitMQ: {message}")
    connection.close()
