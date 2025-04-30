from fastapi import FastAPI
from .producer import send_message_to_kafka, send_message_to_rabbitmq
from .consumer import kafka_listener
import threading

app = FastAPI()

@app.on_event("startup")
def startup_event():
    thread = threading.Thread(target=kafka_listener, daemon=True)
    thread.start()

@app.get("/")
def read_root():
    return {"message": "Hello, World!"}

@app.post("/send_kafka_message/")
async def send_kafka_message(message: str):
    send_message_to_kafka(message)
    return {"status": "Message sent to Kafka"}

@app.post("/send_rabbitmq_message/")
async def send_rabbitmq_message(message: str):
    send_message_to_rabbitmq(message)
    return {"status": "Message sent to RabbitMQ"}
