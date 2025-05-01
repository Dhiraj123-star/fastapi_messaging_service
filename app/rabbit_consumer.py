# app/rabbit_consumer.py
import pika
import json
import os
import time

def rabbitmq_listener():
    rabbit_url = os.getenv('RABBITMQ_URL', 'amqp://guest:guest@rabbitmq:5672/')
    max_retries = int(os.getenv('RABBITMQ_CONSUMER_RETRIES', 5))
    retry_delay = int(os.getenv('RABBITMQ_CONSUMER_RETRY_DELAY', 5))
    
    retries = max_retries
    while retries > 0:
        try:
            params = pika.URLParameters(rabbit_url)
            connection = pika.BlockingConnection(params)
            channel = connection.channel()
            channel.queue_declare(queue='test-queue')

            print("RabbitMQ consumer started... Waiting for messages.")

            def callback(ch, method, properties, body):
                try:
                    message = json.loads(body)
                    print(f"Received from RabbitMQ: {message.get('message')}")
                except Exception as e:
                    print(f"Error decoding RabbitMQ message: {e}")
                ch.basic_ack(delivery_tag=method.delivery_tag)

            channel.basic_consume(queue='test-queue', on_message_callback=callback)
            channel.start_consuming()

        except Exception as e:
            print(f"RabbitMQ consumer error: {e}. Retrying in {retry_delay}s...")
            retries -= 1
            time.sleep(retry_delay)

    if retries == 0:
        print("RabbitMQ consumer failed to connect after retries.")
