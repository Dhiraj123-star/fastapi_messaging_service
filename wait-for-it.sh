#!/bin/bash
# wait-for-it.sh script

host="$1"
shift
cmd="$@"

until nc -z -v -w30 $host 9092; do
  echo "Waiting for $host:9092 to be available..."
  sleep 1
done

echo "$host:9092 is up and ready, starting the command..."
exec $cmd
