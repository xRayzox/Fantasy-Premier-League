#!/bin/bash
set -e

echo "Waiting for Kafka to be ready..."
until nc -z kafka 9092; do
  sleep 1
done

echo "Kafka is ready!"
exec "$@"
