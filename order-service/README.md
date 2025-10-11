# Order Service

Creates orders and publishes `order_created` events to Kafka.

## API Endpoints

- `GET /` - Service info
- `GET /health` - Health check
- `POST /orders` - Create a new order

## Example Usage

Create an order:
```bash
curl -X POST http://localhost:8001/orders \
  -H "Content-Type: application/json" \
  -d '{
    "customer_id": "123",
    "amount": 99.99,
    "items": ["laptop", "mouse"]
  }'
```

## Environment Variables

- `KAFKA_BOOTSTRAP_SERVERS` - Kafka broker address (default: localhost:9092)

## Events Published

- Topic: `order_created`
- Event: Order creation details with order_id, customer_id, amount, items, status, and timestamp