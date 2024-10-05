import json
from airflow.models import Connection
from airflow import settings

# Load connections from the JSON file
with open("/config/airflow/connections.json") as f:
    connections = json.load(f)

# Create a new session
session = settings.Session()

for conn in connections:
    # Check if the connection already exists
    existing_conn = session.query(Connection).filter(Connection.conn_id == conn['conn_id']).first()
    
    if existing_conn:
        print(f"Connection {conn['conn_id']} already exists. Updating the connection.")
        # Update existing connection fields
        for key, value in conn.items():
            setattr(existing_conn, key, value)  # Update the fields of the existing connection
    else:
        print(f"Creating new connection: {conn['conn_id']}")
        # Create a new connection
        new_conn = Connection(**conn)
        session.add(new_conn)

# Commit the session to save changes
session.commit()
print("Connections imported successfully.")
