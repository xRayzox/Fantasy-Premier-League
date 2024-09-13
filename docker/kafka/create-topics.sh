#!/bin/bash
set -e

# Wait for Kafka to be ready
echo "Waiting for Kafka to be ready..."
cub kafka-ready -b kafka:9092 1 30

# List of topics to create
TOPICS=("topic1" "topic2" "topic3")

# Create topics
for TOPIC in "${TOPICS[@]}"; do
  echo "Creating topic $TOPIC..."
  kafka-topics --create --if-not-exists --zookeeper zookeeper:2181 --replication-factor 1 --partitions 1 --topic $TOPIC
done

echo "Topics created successfully!"
