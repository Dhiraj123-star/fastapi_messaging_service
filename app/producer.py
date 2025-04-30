from kafka import KafkaProducer
import json
from pika import BlockingConnection, ConnectionParameters, PlainCredentials

# Kafka producer
def send_message_to_kafka(message: str):
    producer = KafkaProducer(
        bootstrap_servers='kafka:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    # Send message as a dictionary (key: 'message', value: message)
    producer.send('test-topic', {'message': message})
    producer.flush()
    print(f"Message sent to Kafka: {message}")
    producer.close()

# RabbitMQ producer
def send_message_to_rabbitmq(message: str):
    connection = BlockingConnection(ConnectionParameters(
        host='rabbitmq',
        credentials=PlainCredentials('guest', 'guest')
    ))
    channel = connection.channel()
    
    # Declare the queue and send the message
    channel.queue_declare(queue='test-queue')
    channel.basic_publish(exchange='',
                          routing_key='test-queue',
                          body=json.dumps({"message": message}))  # Send as a JSON object
    
    print(f"Message sent to RabbitMQ: {message}")
    connection.close()
