import json
import os
import time
import uuid
from datetime import datetime
from threading import Thread

from fastapi import FastAPI
from kafka import KafkaConsumer, KafkaProducer

app = FastAPI(title="Payment Service", version="1.0.0")

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
CONSUME_TOPIC = "order_created"
PRODUCE_TOPIC = "payment_completed"

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    retries=5,
    retry_backoff_ms=1000
)

def process_payment(order_data):
    """Simulate payment processing"""
    print(f"Processing payment for order: {order_data['order_id']}")
    
    # Simulate processing time
    time.sleep(2)
    
    # Create payment event
    payment_data = {
        "payment_id": str(uuid.uuid4()),
        "order_id": order_data["order_id"],
        "customer_id": order_data["customer_id"],
        "amount": order_data["amount"],
        "status": "completed",
        "processed_at": datetime.utcnow().isoformat()
    }
    
    # Publish payment completed event
    producer.send(PRODUCE_TOPIC, payment_data)
    producer.flush()
    
    print(f"Payment completed for order: {order_data['order_id']}")

def consume_orders():
    """Consume order_created events"""
    consumer = KafkaConsumer(
        CONSUME_TOPIC,
        bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        group_id='payment-service',
        auto_offset_reset='earliest'
    )
    
    print("Payment service started consuming orders...")
    
    for message in consumer:
        try:
            order_data = message.value
            print(f"Received order: {order_data['order_id']}")
            process_payment(order_data)
        except Exception as e:
            print(f"Error processing order: {e}")

# Start consumer in background thread
consumer_thread = Thread(target=consume_orders, daemon=True)
consumer_thread.start()

@app.get("/")
async def root():
    return {"service": "Payment Service", "status": "running"}

@app.get("/health")
async def health():
    return {"status": "healthy"}

@app.on_event("shutdown")
async def shutdown_event():
    producer.close()