#!/bin/bash
set -e

# Function to check if PostgreSQL is ready
wait_for_postgres() {
    echo "Waiting for PostgreSQL to start..."
    until pg_isready -U "$POSTGRES_USER" -d postgres; do
        sleep 2
    done
}

# Function to check if a database exists and create it if not
check_and_create_db() {
    local dbname=$1
    echo "Checking for database: $dbname"

    if psql -U "$POSTGRES_USER" -d postgres -lqt | cut -d \| -f 1 | grep -qw "$dbname"; then
        echo "Database $dbname already exists, skipping creation."
    else
        echo "Creating database $dbname."
        createdb -U "$POSTGRES_USER" "$dbname"
        echo "Database $dbname created."
    fi
}

# Wait for PostgreSQL to be ready
wait_for_postgres

# Create databases
check_and_create_db "airflow"
check_and_create_db "test_postgres"
