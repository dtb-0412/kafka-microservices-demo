#!/bin/bash

echo "Kafka Topics Monitor"
echo "==================="

echo ""
echo "Available topics:"
docker-compose exec kafka kafka-topics --bootstrap-server localhost:9092 --list

echo ""
echo "Monitoring order_created topic (press Ctrl+C to stop):"
echo "------------------------------------------------------"
docker-compose exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic order_created \
  --from-beginning \
  --property print.timestamp=true