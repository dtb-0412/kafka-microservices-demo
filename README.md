# Event-Driven Microservices with Apache Kafka

🔗 **Repository**: https://github.com/Copubah/kafka-microservices-demo

A simple demonstration of event-driven architecture using Apache Kafka with three microservices:

- Order Service: Creates orders and publishes `order_created` events
- Payment Service: Processes payments and publishes `payment_completed` events  
- Notification Service: Sends notifications when payments are completed

## Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Order Service │    │ Payment Service │    │Notification Svc │
│   (Port 8001)   │    │   (Port 8002)   │    │   (Port 8003)   │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         │ publishes             │ publishes             │ consumes
         │                       │                       │
         ▼                       ▼                       ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│ order_created   │    │payment_completed│    │     Logs        │
│     Topic       │    │     Topic       │    │  Notification   │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       ▲
         │ consumes              │
         │                       │
         └───────────────────────┘

                    ┌─────────────────┐
                    │  Apache Kafka   │
                    │   + Zookeeper   │
                    │   (Port 9092)   │
                    └─────────────────┘
                            │
                    ┌─────────────────┐
                    │    Kafka UI     │
                    │   (Port 8080)   │
                    └─────────────────┘

Event Flow:
1. HTTP POST /orders → Order Service
2. Order Service → order_created topic
3. Payment Service consumes → processes payment
4. Payment Service → payment_completed topic  
5. Notification Service consumes → logs notification
```

## Quick Start

1. Start all services:
```bash
docker-compose up --build
```

2. Create an order (in another terminal):
```bash
curl -X POST http://localhost:8001/orders \
  -H "Content-Type: application/json" \
  -d '{"customer_id": "123", "amount": 99.99, "items": ["laptop", "mouse"]}'
```

3. Watch the logs to see the event flow:
```bash
docker-compose logs -f
```

## Services

- Order Service: http://localhost:8001
- Payment Service: http://localhost:8002  
- Notification Service: http://localhost:8003
- Kafka UI: http://localhost:8080 (optional web interface)

## Testing the Event Flow

### Automated Testing
Run the test script to create sample orders:
```bash
./test-orders.sh
```

### Manual Testing
The services communicate through Kafka topics:

1. POST to Order Service creates an order
2. Order Service publishes to `order_created` topic
3. Payment Service consumes the event and processes payment
4. Payment Service publishes to `payment_completed` topic
5. Notification Service consumes and logs the notification

### Sample Test Commands

Create different types of orders:
```bash
# Electronics order
curl -X POST http://localhost:8001/orders \
  -H "Content-Type: application/json" \
  -d '{"customer_id": "customer_123", "amount": 1299.99, "items": ["laptop", "wireless mouse"]}'

# Books order  
curl -X POST http://localhost:8001/orders \
  -H "Content-Type: application/json" \
  -d '{"customer_id": "customer_456", "amount": 45.50, "items": ["Python book", "JavaScript book"]}'
```

## Monitoring and Debugging

### View Logs
Watch all service logs:
```bash
docker-compose logs -f
```

Watch specific service:
```bash
docker-compose logs -f order-service
```

### Monitor Kafka Topics
Use the monitoring script:
```bash
./monitor-topics.sh
```

### Manual Kafka Commands

Access Kafka container:
```bash
docker-compose exec kafka bash
```

List topics:
```bash
kafka-topics --bootstrap-server localhost:9092 --list
```

Consume messages from beginning:
```bash
kafka-console-consumer --bootstrap-server localhost:9092 --topic order_created --from-beginning
kafka-console-consumer --bootstrap-server localhost:9092 --topic payment_completed --from-beginning
```

### Kafka UI
Access the web interface at http://localhost:8080 to view topics, messages, and consumer groups.

## Best Practices

### Event Design
- Use clear, descriptive event names (order_created, payment_completed)
- Include all necessary data in events to avoid service coupling
- Use consistent event schema with version information
- Add correlation IDs for tracing events across services
- Include timestamps for event ordering and debugging

### Error Handling
- Implement retry logic with exponential backoff
- Use dead letter queues for failed messages
- Log all errors with context for debugging
- Implement circuit breakers for external dependencies
- Handle duplicate messages (idempotency)

### Kafka Configuration
- Set appropriate replication factor (min 3 for production)
- Configure proper retention policies for topics
- Use consumer groups for load balancing
- Set reasonable batch sizes and timeouts
- Monitor consumer lag and partition distribution

### Service Design
- Keep services small and focused on single responsibility
- Use health checks for service monitoring
- Implement graceful shutdown handling
- Separate business logic from infrastructure code
- Use environment variables for configuration

### Security
- Enable SSL/TLS for Kafka communication
- Implement authentication and authorization
- Use secrets management for sensitive data
- Network isolation between services
- Regular security updates and patches

### Monitoring and Observability
- Implement structured logging with correlation IDs
- Add metrics for message throughput and latency
- Use distributed tracing for request flows
- Monitor consumer lag and processing times
- Set up alerts for critical failures

### Production Considerations
- Use multiple Kafka brokers for high availability
- Implement proper backup and disaster recovery
- Scale consumers based on partition count
- Use schema registry for event evolution
- Implement proper testing strategies (unit, integration, contract)

### Development Workflow
- Use feature flags for gradual rollouts
- Implement database per service pattern
- Use API versioning for backward compatibility
- Automate deployment pipelines
- Maintain comprehensive documentation

## Project Structure

```
├── order-service/
├── payment-service/
├── notification-service/
├── docker-compose.yml
└── README.md
```