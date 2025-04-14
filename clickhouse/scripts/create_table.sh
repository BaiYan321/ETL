#!/bin/bash

# Start clickhouse in the background
/entrypoint.sh &

# Wait for ClickHouse to start
until curl -s http://localhost:8123/ | grep -q "Ok"; do
    echo "Waiting for ClickHouse..."
    sleep 5
done

# Define ClickHouse server and database parameters
CLICKHOUSE_HOST="${CLICKHOUSE_HOST:-localhost}"
CLICKHOUSE_PORT="${CLICKHOUSE_PORT:-8123}"
CLICKHOUSE_DB="${CLICKHOUSE_DB:-marketstack_db}"
CLICKHOUSE_USER="${CLICKHOUSE_USER:-default}"
CLICKHOUSE_PASSWORD="${CLICKHOUSE_PASSWORD:-default}"

# Create database if it does not exist
curl -u $CLICKHOUSE_USER:$CLICKHOUSE_PASSWORD "http://$CLICKHOUSE_HOST:$CLICKHOUSE_PORT" --data-binary "CREATE DATABASE IF NOT EXISTS $CLICKHOUSE_DB"

# Create the marketstack_streaming table
curl -u $CLICKHOUSE_USER:$CLICKHOUSE_PASSWORD "http://$CLICKHOUSE_HOST:$CLICKHOUSE_PORT" --data-binary "
CREATE TABLE IF NOT EXISTS $CLICKHOUSE_DB.marketstack_streaming (
  Symbol String,
  Date Date,
  Open Float32,
  Close Float32,
  Low Float32,
  High Float32,
  Volume Int32,
  Growth Float32,
  GrowthPct Float32,
  Fluctuation Float32
) ENGINE = MergeTree()
ORDER BY (Symbol, Date)
PRIMARY KEY Symbol
SETTINGS index_granularity = 8192; 
"

# Create the processed_marketstack table
curl -u $CLICKHOUSE_USER:$CLICKHOUSE_PASSWORD "http://$CLICKHOUSE_HOST:$CLICKHOUSE_PORT" --data-binary "
CREATE TABLE IF NOT EXISTS $CLICKHOUSE_DB.processed_marketstack (
  Symbol String,
  Date Date,
  Open Float32,
  Close Float32,
  Low Float32,
  High Float32,
  Volume Int32,
  Growth Float32,
  GrowthPct Float32,
  Fluctuation Float32
) ENGINE = MergeTree()
ORDER BY (Symbol, Date)
PRIMARY KEY Symbol
SETTINGS index_granularity = 8192;
"

echo "Database and tables have been successfully set up."
