# Arguments for Airflow and Python versions
ARG AIRFLOW_VERSION
ARG AIRFLOW_PYTHON_VERSION

# Declare ARG before FROM and use them inside the FROM
FROM apache/airflow:${AIRFLOW_VERSION}-python${AIRFLOW_PYTHON_VERSION}

# Set working directory
WORKDIR /airflow/

COPY /airflow/cfg/requirements.txt  requirements.txt
# Use ARG variables in the RUN command
ARG AIRFLOW_VERSION
ARG AIRFLOW_PYTHON_VERSION

# Install Python dependencies from requirements.txt
RUN pip install --no-cache-dir -r requirements.txt \
    --constraint https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${AIRFLOW_PYTHON_VERSION}.txt

# Switch to root user to install packages
USER root

# Install supervisord and dos2unix
RUN apt-get update && apt-get install -y \
    supervisor \
    dos2unix && \
    rm -rf /var/lib/apt/lists/*

# Create directories for Supervisor's configuration and logs
RUN mkdir -p /airflow/app/logs /etc/supervisor/conf.d

# Copy the Supervisor configuration file to the appropriate directory
COPY supervisord.conf /etc/supervisor/conf.d/supervisord.conf

# Convert the supervisord.conf file to Unix format using dos2unix
RUN dos2unix /etc/supervisor/conf.d/supervisord.conf

# Copy initialization script
COPY ./postgres_data/init-db.sh /docker-entrypoint-initdb.d/init-db.sh

# Ensure the script is executable
RUN dos2unix /docker-entrypoint-initdb.d/init-db.sh
RUN chmod +x /docker-entrypoint-initdb.d/init-db.sh

# Ensure /airflow/dags directory exists and convert line endings for DAG files
RUN mkdir -p /airflow/dags && find /airflow/dags -type f -exec dos2unix {} \;
RUN mkdir -p /airflow/plugins && find /airflow/plugins -type f -exec dos2unix {} \;

RUN chmod -R 755 /airflow/app/logs
RUN chmod -R 755 /airflow/dags
RUN chmod -R 755 /airflow/plugins

# Set Airflow user back for security
USER airflow

# Expose the Airflow webserver port
EXPOSE 8080

# Default entrypoint is Supervisor to run multiple processes
ENTRYPOINT ["/bin/bash"]

# ENTRYPOINT ["supervisord", "-c", "/etc/supervisor/conf.d/supervisord.conf"]
