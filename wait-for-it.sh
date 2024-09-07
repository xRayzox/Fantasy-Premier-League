#!/usr/bin/env bash
# wait-for-it.sh

# This script waits for a specific host and port to become available.

host="$1"
shift
port="$1"
shift

timeout=${WAITFORIT_TIMEOUT:-15}
cmd="$@"

echo "Waiting for $host:$port..."

until nc -z $host $port; do
  sleep 1
  timeout=$(($timeout - 1))
  if [ "$timeout" -le 0 ]; then
    echo "Timed out waiting for $host:$port"
    exit 1
  fi
done

echo "$host:$port is available. Executing command..."
exec $cmd
