#!/bin/bash

echo "Testing Event-Driven Microservices..."
echo "======================================"

# Wait for services to be ready
echo "Waiting for services to start..."
sleep 5

echo ""
echo "Creating test orders..."
echo ""

# Test order 1
echo "1. Creating order for laptop and mouse..."
curl -X POST http://localhost:8001/orders \
  -H "Content-Type: application/json" \
  -d '{
    "customer_id": "customer_123",
    "amount": 1299.99,
    "items": ["laptop", "wireless mouse"]
  }' | jq .

echo ""
sleep 3

# Test order 2
echo "2. Creating order for books..."
curl -X POST http://localhost:8001/orders \
  -H "Content-Type: application/json" \
  -d '{
    "customer_id": "customer_456", 
    "amount": 45.50,
    "items": ["Python book", "JavaScript book"]
  }' | jq .

echo ""
sleep 3

# Test order 3
echo "3. Creating order for coffee..."
curl -X POST http://localhost:8001/orders \
  -H "Content-Type: application/json" \
  -d '{
    "customer_id": "customer_789",
    "amount": 12.99,
    "items": ["premium coffee beans"]
  }' | jq .

echo ""
echo "Orders created! Check the docker-compose logs to see the event flow:"
echo "docker-compose logs -f"