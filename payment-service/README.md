# Payment Service

Consumes `order_created` events, processes payments, and publishes `payment_completed` events.

## API Endpoints

- `GET /` - Service info
- `GET /health` - Health check

## Functionality

1. Listens for `order_created` events from Kafka
2. Simulates payment processing (2-second delay)
3. Publishes `payment_completed` events

## Environment Variables

- `KAFKA_BOOTSTRAP_SERVERS` - Kafka broker address (default: localhost:9092)

## Events Consumed

- Topic: `order_created`
- Consumer Group: `payment-service`

## Events Published

- Topic: `payment_completed`
- Event: Payment completion details with payment_id, order_id, customer_id, amount, status, and timestamp