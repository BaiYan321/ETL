#!/bin/bash

# Start Spark master in the background
/opt/bitnami/spark/sbin/start-master.sh &

# Wait for the Spark master to be ready
until $(curl --output /dev/null --silent --head --fail http://spark-master:8080); do
    echo "Waiting for Spark master to start..."
    sleep 5
done

echo "Spark master is up and running!"

# Now submit the Spark job
/opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --num-executors 1 \
  --total-executor-cores 1 \
  --executor-memory 1g \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.apache.kafka:kafka-clients:2.8.1 \
  /usr/local/spark/python_code/spark_streaming.py
