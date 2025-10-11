# Troubleshooting Guide

## Common Issues

### Services Not Starting
Problem: Services fail to start or keep restarting

Solutions:
1. Check if ports are available:
   ```bash
   netstat -tulpn | grep -E ':(8001|8002|8003|9092|2181)'
   ```

2. Restart with fresh containers:
   ```bash
   docker-compose down -v
   docker-compose up --build
   ```

3. Check service logs:
   ```bash
   docker-compose logs order-service
   ```

### Kafka Connection Issues
Problem: Services can't connect to Kafka

Solutions:
1. Wait for Kafka to be fully ready (can take 30-60 seconds)
2. Check Kafka is running:
   ```bash
   docker-compose ps kafka
   ```

3. Verify Kafka topics are created:
   ```bash
   docker-compose exec kafka kafka-topics --bootstrap-server localhost:9092 --list
   ```

### No Events Flowing
Problem: Orders created but no payment/notification events

Solutions:
1. Check consumer group status:
   ```bash
   docker-compose exec kafka kafka-consumer-groups --bootstrap-server localhost:9092 --list
   ```

2. Reset consumer groups:
   ```bash
   docker-compose exec kafka kafka-consumer-groups --bootstrap-server localhost:9092 --group payment-service --reset-offsets --to-earliest --topic order_created --execute
   ```

3. Restart consumer services:
   ```bash
   docker-compose restart payment-service notification-service
   ```

### Port Conflicts
Problem: "Port already in use" errors

Solutions:
1. Stop conflicting services:
   ```bash
   sudo lsof -ti:8001 | xargs kill -9
   ```

2. Use different ports by modifying docker-compose.yml

### Memory Issues
Problem: Services running slowly or crashing

Solutions:
1. Increase Docker memory limits
2. Reduce Kafka retention settings
3. Monitor resource usage:
   ```bash
   docker stats
   ```

## Health Checks

Check if all services are healthy:
```bash
curl http://localhost:8001/health  # Order Service
curl http://localhost:8002/health  # Payment Service  
curl http://localhost:8003/health  # Notification Service
```

## Reset Everything
If all else fails, completely reset:
```bash
docker-compose down -v --remove-orphans
docker system prune -f
docker-compose up --build
```