import json
import os
from threading import Thread

from fastapi import FastAPI
from kafka import KafkaConsumer

app = FastAPI(title="Notification Service", version="1.0.0")

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
CONSUME_TOPIC = "payment_completed"

def send_notification(payment_data):
    """Simulate sending notification"""
    customer_id = payment_data["customer_id"]
    order_id = payment_data["order_id"]
    amount = payment_data["amount"]
    
    notification_message = (
        f"Payment Successful!\n"
        f"Customer: {customer_id}\n"
        f"Order: {order_id}\n"
        f"Amount: ${amount:.2f}\n"
        f"Your order has been confirmed and will be processed shortly."
    )
    
    print("=" * 50)
    print("NOTIFICATION SENT:")
    print(notification_message)
    print("=" * 50)

def consume_payments():
    """Consume payment_completed events"""
    consumer = KafkaConsumer(
        CONSUME_TOPIC,
        bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        group_id='notification-service',
        auto_offset_reset='latest'
    )
    
    print("Notification service started consuming payments...")
    
    for message in consumer:
        try:
            payment_data = message.value
            print(f"Received payment completion: {payment_data['payment_id']}")
            send_notification(payment_data)
        except Exception as e:
            print(f"Error processing payment notification: {e}")

# Start consumer in background thread
consumer_thread = Thread(target=consume_payments, daemon=True)
consumer_thread.start()

@app.get("/")
async def root():
    return {"service": "Notification Service", "status": "running"}

@app.get("/health")
async def health():
    return {"status": "healthy"}