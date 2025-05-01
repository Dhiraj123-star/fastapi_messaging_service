# producers.py
from kafka import KafkaProducer
import json
from pika import BlockingConnection, ConnectionParameters, PlainCredentials

# Kafka producer
def send_message_to_kafka(message: str):
    bootstrap_servers = 'kafka:9092'  # Hardcoded for now
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
    rabbitmq_host = 'rabbitmq'           # Hardcoded
    rabbitmq_port = 5672                 # Default port
    rabbitmq_user = 'guest'              # Default user
    rabbitmq_password = 'guest'          # Default password
    rabbitmq_vhost = '/'                 # Default virtual host

    credentials = PlainCredentials(rabbitmq_user, rabbitmq_password)
    connection = BlockingConnection(ConnectionParameters(
        host=rabbitmq_host,
        port=rabbitmq_port,
        virtual_host=rabbitmq_vhost,
        credentials=credentials
    ))

    channel = connection.channel()
    channel.queue_declare(queue='test-queue')
    channel.basic_publish(
        exchange='',
        routing_key='test-queue',
        body=json.dumps({"message": message})
    )
    print(f"Message sent to RabbitMQ: {message}")
    connection.close()
