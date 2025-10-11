# Notification Service

Consumes `payment_completed` events and sends notifications to customers.

## API Endpoints

- `GET /` - Service info
- `GET /health` - Health check

## Functionality

1. Listens for `payment_completed` events from Kafka
2. Simulates sending notifications (logs to console)
3. Displays formatted notification messages

## Environment Variables

- `KAFKA_BOOTSTRAP_SERVERS` - Kafka broker address (default: localhost:9092)

## Events Consumed

- Topic: `payment_completed`
- Consumer Group: `notification-service`

## Sample Notification Output

```
==================================================
NOTIFICATION SENT:
Payment Successful!
Customer: 123
Order: abc-123-def
Amount: $99.99
Your order has been confirmed and will be processed shortly.
==================================================
```