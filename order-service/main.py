import json
import os
import uuid
from datetime import datetime
from typing import List

from fastapi import FastAPI, HTTPException
from kafka import KafkaProducer
from pydantic import BaseModel

app = FastAPI(title="Order Service", version="1.0.0")

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = "order_created"

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    retries=5,
    retry_backoff_ms=1000
)

class OrderRequest(BaseModel):
    customer_id: str
    amount: float
    items: List[str]

class OrderResponse(BaseModel):
    order_id: str
    customer_id: str
    amount: float
    items: List[str]
    status: str
    created_at: str

@app.get("/")
async def root():
    return {"service": "Order Service", "status": "running"}

@app.get("/health")
async def health():
    return {"status": "healthy"}

@app.post("/orders", response_model=OrderResponse)
async def create_order(order: OrderRequest):
    try:
        # Generate order
        order_id = str(uuid.uuid4())
        created_at = datetime.utcnow().isoformat()
        
        order_data = {
            "order_id": order_id,
            "customer_id": order.customer_id,
            "amount": order.amount,
            "items": order.items,
            "status": "created",
            "created_at": created_at
        }
        
        # Publish to Kafka
        producer.send(KAFKA_TOPIC, order_data)
        producer.flush()
        
        print(f"Order created and published: {order_id}")
        
        return OrderResponse(**order_data)
        
    except Exception as e:
        print(f"Error creating order: {e}")
        raise HTTPException(status_code=500, detail="Failed to create order")

@app.on_event("shutdown")
async def shutdown_event():
    producer.close()