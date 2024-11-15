

# ETL Pipeline Project

This project demonstrates a robust ETL pipeline architecture that ingests data from external APIs, processes it, and stores it in optimized data storage solutions. The pipeline leverages a variety of technologies for data streaming, processing, and analytics. Below, we detail each component of the system, its role, and how they integrate to form a comprehensive ETL pipeline.

![ETL](https://github.com/user-attachments/assets/e4a8033e-a66f-4ecd-88d8-a643bc48b400)
---

## 📋 Table of Contents

- [Architecture Overview](#architecture-overview)
- [Technologies Used](#technologies-used)
- [Pipeline Flow](#pipeline-flow)
- [Installation and Setup](#installation-and-setup)
- [Usage](#usage)
- [Contributing](#contributing)
- [License](#license)

---

## 🏗️ Architecture Overview

The ETL pipeline is containerized using Docker for ease of deployment and scalability. The data ingestion and processing flow can be broken down into the following components:

1. **Data Source (Marketstack API)**: 
   - Fetches financial data from Marketstack API.

2. **Apache Kafka & ZooKeeper**:
   - Kafka acts as the data streaming platform that collects real-time data.
   - ZooKeeper manages Kafka brokers and maintains cluster health.

3. **PySpark**:
   - Performs data transformations on the ingested data to make it analytics-ready.

4. **Data Storage**:
   - **ClickHouse**: Stores transformed data for fast analytics and reporting.
   - **PostgreSQL**: Serves as a long-term storage solution for structured data.

5. **Monitoring & Logging**:
   - **Fluentd** collects logs from different services.
   - **ElasticSearch & Kibana** provide log indexing and visualization.
   - **Grafana** monitors the health of the pipeline.
   - **SendGrid** sends notifications for pipeline alerts.

---

## 🛠️ Technologies Used

- **Docker**: Containerization for easy deployment.
- **Marketstack API**: Financial data provider.
- **Apache Kafka & ZooKeeper**: Data streaming and management.
- **PySpark**: Data processing and transformation.
- **ClickHouse**: High-performance OLAP database for analytics.
- **PostgreSQL**: Relational database for structured data storage.
- **Fluentd**: Log collector and processor.
- **ElasticSearch & Kibana**: Log management and visualization.
- **Grafana**: Monitoring dashboards.
- **SendGrid**: Email alerts for pipeline monitoring.

---

## 🚀 Pipeline Flow

1. **Data Ingestion**:
   - The pipeline fetches financial data using the Marketstack API.
   - The data is sent to Kafka, which acts as a data stream.

2. **Data Transformation**:
   - PySpark processes the streaming data from Kafka.
   - The transformed data is loaded into ClickHouse for fast queries and PostgreSQL for long-term storage.

3. **Monitoring & Logging**:
   - Logs from all services are collected using Fluentd.
   - Logs are indexed in ElasticSearch and visualized using Kibana.
   - Grafana dashboards monitor system health and performance.
   - SendGrid sends alerts if any part of the pipeline fails.

---

## ⚙️ Installation and Setup

To run this project, ensure you have **Docker** and **Docker Compose** installed.

### Clone the Repository
```bash
git clone https://github.com/BaiYan321/etl.git
cd etl
```

### Update Environment Variables
Create a `.env` file based on the `.env.example` file and add your **Marketstack API key**, **Kafka**, **ClickHouse**, **PostgreSQL**, and **SendGrid** credentials.

### Build and Start the Docker Containers
```bash
docker-compose up --build
```

### Accessing the Services
- Airflow: [http://localhost:8080](http://localhost:8080)
- Kafka: [http://localhost:9000](http://localhost:9000)
- Postgres: [http://localhost:5432](http://localhost:5432)
- Spark: [http://spark://spark-master:7077](http://spark://spark-master:7077)
- ClickHouse: [http://localhost:8123/play](http://localhost:8123/play)
- Kibana: [http://localhost:5601](http://localhost:5601)
- Grafana: [http://localhost:3000](http://localhost:3000)


---

## 📊 Usage

1. **Run the ETL Pipeline**:
   - The pipeline will automatically start fetching and processing data once the containers are up.

2. **Monitoring**:
   - Use Grafana to monitor logs.
   - Use Kibana to view and analyze logs.
   - Receive notifications via SendGrid if the pipeline encounters issues.

---

## 🤝 Contributing

We welcome contributions! Please submit a pull request or open an issue to discuss any changes.

---

## 📝 License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for more details.

---

## 📧 Contact

If you have any questions, feel free to reach out at **your-email@example.com**.
