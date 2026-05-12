#!/bin/bash

sleep 3

echo "Starting Topic Creation"

topics=(
  "breaking-news-alerts"
  "bot-alerts"
  "spam-alerts"
  ""
)

for topic in "${topics[@]}"; do
  ./opt/kafka/bin/kafka-topics.sh --create --topic $topic --partitions 1 --replication-factor 1 --bootstrap-server localhost:9092
done

echo "Topic creation done"