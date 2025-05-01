# main.py
from fastapi import FastAPI
from pydantic import BaseModel
from .producer import send_message_to_kafka, send_message_to_rabbitmq
from .consumer import kafka_listener
import threading

app = FastAPI()

# Define a Pydantic model to parse the request body
class MessageRequest(BaseModel):
    message: str

@app.on_event("startup")
def startup_event():
    print("Starting Kafka listener thread...")
    
    def thread_target():
        try:
            kafka_listener()
        except Exception as e:
            print(f"Kafka listener thread crashed: {e}")
    
    # Start the Kafka listener in a separate thread
    thread = threading.Thread(target=thread_target, daemon=True)
    thread.start()

@app.get("/")
def read_root():
    return {"message": "Hello, World!"}

@app.post("/send_kafka_message/")
async def send_kafka_message(request: MessageRequest):
    send_message_to_kafka(request.message)
    return {"status": "Message sent to Kafka"}

@app.post("/send_rabbitmq_message/")
async def send_rabbitmq_message(request: MessageRequest):
    send_message_to_rabbitmq(request.message)
    return {"status": "Message sent to RabbitMQ"}

@app.get("/health")
def health():
    return {"status": "ok"}
