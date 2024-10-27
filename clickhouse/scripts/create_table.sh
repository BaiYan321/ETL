#!/bin/bash -e

# Start Spark master in the background
/entrypoint.sh &
# ./entrypoint.sh & # not working
# entrypoint.sh & # not working

# Wait for the Spark master to be ready
# until $(curl --output /dev/null --silent --head --fail http://localhost:8123); do
#     echo "Waiting for Clickhouse to start..."
#     sleep 5
# done
until curl -s http://clickhouse:8123/ | grep -q "Ok"; do
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

# SQL to create a table
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
ORDER BY Symbol;"

echo "Database and table setup completed."