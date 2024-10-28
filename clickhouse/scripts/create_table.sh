#!/bin/bash -e

# Start clickhouse in the background
/entrypoint.sh &

until curl -s http://localhost:8123/ | grep -q "Ok"; do
    echo "Waiting for ClickHouse..."
    sleep 5
done

# Define the ClickHouse server and database
CLICKHOUSE_HOST="${CLICKHOUSE_HOST:-localhost}"
CLICKHOUSE_PORT="${CLICKHOUSE_PORT:-8123}"
CLICKHOUSE_DB="${CLICKHOUSE_DB:-marketstack_db}"
CLICKHOUSE_USER="${CLICKHOUSE_USER:-default}"
CLICKHOUSE_PASSWORD="${CLICKHOUSE_PASSWORD:-default}"

# SQL to create a database if it does not exist
curl -u $CLICKHOUSE_USER:$CLICKHOUSE_PASSWORD "http://$CLICKHOUSE_HOST:$CLICKHOUSE_PORT" --data-binary "CREATE DATABASE IF NOT EXISTS $CLICKHOUSE_DB"

# # SQL to create a marketstack_streaming
 curl -u default:default "http://localhost:8123" --data-binary "
 CREATE TABLE IF NOT EXISTS marketstack_db.marketstack_streaming (
 	Symbol String,
 	Date Date,
 	Open Float32,
 	Close Float32,
 	Low Float32,
 	High Float32,
 	Volume INT,
 	Growth Float32,
 	GrowthPct Float32,
 	Fluctuation Float32
 )   ENGINE = MergeTree()
 ORDER BY (Symbol, Date)
 PRIMARY KEY Symbol
 SETTINGS index_granularity = 8192; 
 "

# # SQL to create a processed_marketstack
 curl -u default:default "http://localhost:8123" --data-binary "
 CREATE TABLE IF NOT EXISTS marketstack_db.processed_marketstack (
 	Symbol String,
 	Date Date,
 	Open Float32,
 	Close Float32,
 	Low Float32,
 	High Float32,
 	Volume INT,
 	Growth Float32,
 	GrowthPct Float32,
 	Fluctuation Float32
 )   ENGINE = MergeTree()
 ORDER BY (Symbol, Date)
 PRIMARY KEY Symbol
 SETTINGS index_granularity = 8192; 
 "

echo "Database and table setup completed."

# Keep container running
tail -f /dev/null