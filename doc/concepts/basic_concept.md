## 1. PowerShell, CMD, and Bash

### Concept

- **PowerShell**: A task automation framework from Microsoft, consisting of a command-line shell and scripting language. It is more powerful than CMD and is widely used for automation in Windows environments.
- **CMD (Command Prompt)**: A default command-line interpreter for Windows OS that executes entered commands and is used for executing batch scripts.
- **Bash (Bourne Again SHell)**: A Unix shell and command language, widely used in Linux and macOS for scripting and automation. It offers powerful scripting capabilities and is the default shell in most Linux distributions.

### Differences Between PowerShell, CMD, and Bash

| Feature                  | PowerShell                                | CMD                                    | Bash                                   |
|--------------------------|--------------------------------------------|-----------------------------------------|-----------------------------------------|
| **Platform**             | Windows (cross-platform with Core)         | Windows only                           | Linux, macOS, Windows (via WSL)        |
| **Scripting Language**   | Advanced scripting language with .NET      | Basic batch scripting                  | Advanced scripting language (shell scripts) |
| **Object-Oriented**      | Yes (works with objects)                   | No (text-based)                        | No (text-based)                        |
| **Piping**               | Objects can be piped between commands      | Only text streams                      | Text streams                           |
| **Command Syntax**       | Uses cmdlets (`Get-Process`, `Set-Item`)   | DOS commands (`dir`, `cd`)             | UNIX-like commands (`ls`, `cd`)        |
| **Command Compatibility**| Compatible with cmdlets and some Linux commands | DOS commands only                      | Compatible with most UNIX commands     |
| **Power and Flexibility**| High; integrates with .NET libraries and supports advanced automation | Limited                                | High; supports advanced automation, regular expressions, etc. |
| **Script Extension**     | `.ps1`                                     | `.bat`, `.cmd`                         | `.sh`                                  |

### Examples

- **PowerShell**:
  - List all running processes: `Get-Process`
  - Copy files: `Copy-Item -Path "source" -Destination "destination"`
  - Iterate over a list: `foreach ($item in $list) { Write-Output $item }`

- **CMD**:
  - List directory contents: `dir`
  - Change directory: `cd folder_name`
  - Simple for loop: `FOR %i IN (1 2 3) DO echo %i`

- **Bash**:
  - List files: `ls -l`
  - Copy files: `cp source destination`
  - For loop: `for i in {1..3}; do echo $i; done`

---

## 2. Distributed Systems Concept and Storage

### Concept

- **Distributed Systems**: A distributed system is a network of independent computers that communicate and coordinate their actions by passing messages to one another. The goal is to achieve a common task. Examples include cloud computing systems, microservices architectures, and blockchain networks.
- **Storage in Distributed Systems**:
  - **Centralized Storage**: Data is stored in a single location, which all nodes can access.
  - **Distributed Storage**: Data is spread across multiple nodes in the system. Examples are HDFS (Hadoop Distributed File System), Amazon S3, and Google Cloud Storage.
  - **Replication and Sharding**: Two main techniques used to manage data in distributed storage systems. Replication involves duplicating data across multiple nodes for fault tolerance, while sharding involves splitting data into chunks that are stored across different nodes for scalability.

### Examples of Systems or Frameworks

- **Apache Hadoop**: A distributed data processing framework using the Hadoop Distributed File System (HDFS) for distributed storage and MapReduce for distributed computation.
- **Apache Kafka**: A distributed streaming platform that allows for high-throughput, low-latency ingestion of data streams in real-time. It is widely used for building real-time analytics and event-driven architectures.
- **Apache Cassandra**: A distributed NoSQL database designed to handle large amounts of data across many commodity servers, providing high availability with no single point of failure.
- **Google Cloud Bigtable**: A distributed storage system by Google designed to manage petabytes of data and can be scaled horizontally. It is often used for time-series data, IoT data, and analytical workloads.
- **Amazon DynamoDB**: A serverless, key-value, and document NoSQL database service that is fully managed, scalable, and supports distributed storage and high availability.

---

## 3. Data Warehouse and Building Data Models in Data Warehouse

### Concept

- **Data Warehouse**: A centralized repository designed to store large amounts of structured data from different sources, primarily for analysis and reporting. It uses Online Analytical Processing (OLAP) to enable complex queries and data analysis.
- **Building Data Models in Data Warehouse**:
  - **Star Schema**: A simple, de-normalized schema that consists of a central fact table connected to dimension tables.
  - **Snowflake Schema**: A more normalized form of the star schema where dimension tables can have their sub-dimension tables.
  - **Data Vault**: A hybrid approach for designing data warehouses, combining both normalization and denormalization techniques.
  - **ETL (Extract, Transform, Load)**: The process used to build and maintain data models in data warehouses by extracting data from source systems, transforming it to fit the data model, and loading it into the data warehouse.

### Examples of Data Warehouses and Data Models

- **Data Warehouse Examples**:
  - **Amazon Redshift**: A cloud-based data warehousing service optimized for complex queries and analytics. It allows integration with various data sources, making it a popular choice for scalable analytics.
  - **Google BigQuery**: A fully managed, serverless, and highly scalable data warehouse that allows running super-fast SQL queries on large datasets.
  - **Snowflake**: A cloud-based data warehousing solution that provides a highly scalable architecture and separates storage from computing, optimizing for cost and performance.

- **Data Models**:
  - **Star Schema**:
    - **Fact Table**: Contains quantitative data (e.g., sales amount, units sold).
    - **Dimension Tables**: Contain descriptive attributes (e.g., product, time, location).
    - **Example**: A sales data warehouse where the central `sales` table is connected to `product`, `customer`, and `time` dimension tables.
  - **Snowflake Schema**:
    - More normalized than the star schema. Dimension tables are split into more granular tables.
    - **Example**: In a snowflake schema, the `product` dimension table might be split into `product` and `category` tables.
  - **Data Vault**:
    - Combines both normalization and denormalization. It has three types of tables: hubs (core entities), links (relationships), and satellites (descriptive data).
    - **Example**: A data vault model where a `CustomerHub` table contains unique customer IDs, a `CustomerDetailsSat` table contains customer attributes, and an `OrderLink` table connects customer and order hubs.

---

## 4. Lakehouse Concept and Delta Lake

### Concept

- **Lakehouse Concept**: A new data architecture concept that combines the best features of data lakes and data warehouses. The lakehouse architecture supports ACID transactions, data versioning, and schema enforcement directly on a data lake. It allows for both data science and business analytics use cases from a single data source.
- **Delta Lake**: An open-source storage layer that brings ACID (Atomicity, Consistency, Isolation, Durability) transactions to data lakes. It enables reliable and scalable data engineering pipelines by providing features such as:
  - **ACID Transactions**: Ensures data consistency and reliability.
  - **Schema Enforcement and Evolution**: Provides better control over data structure.
  - **Time Travel**: Allows users to query older versions of data.
  - **Scalability and Performance**: Optimized for both batch and streaming data.

### Delta Lake Examples

- **Data Lake Management with Delta Lake**:
  - Delta Lake is used to manage and optimize data lakes by providing ACID transactions, schema enforcement, and the ability to handle both batch and streaming data.
  - **Example**: A Delta Lake implementation on top of an existing Azure Data Lake Storage (ADLS) or Amazon S3 storage. This allows companies to perform data engineering, data science, and machine learning workflows seamlessly.
- **Time Travel with Delta Lake**:
  - **Scenario**: A financial organization wants to analyze how their portfolio looked at a specific date in the past.
  - **Delta Lake Command**: `SELECT * FROM transactions VERSION AS OF 5;` – This retrieves data from a specific version (snapshot) of the Delta Table, allowing users to "travel back in time" and see how the data looked at that point.
- **Schema Evolution and Enforcement**:
  - **Scenario**: As a company's user data evolves (e.g., adding a new column `phone_number`), Delta Lake allows for safe schema evolution.
  - **Delta Lake Command**: `ALTER TABLE user_data ADD COLUMNS (phone_number STRING);` – This command safely adds a new column to an existing Delta Table, enforcing the new schema.

---

## 5. Data Governance

### Concept

- **Data Governance**: The process of managing the availability, usability, integrity, and security of the data used in an organization. It includes policies, procedures, roles, and responsibilities to ensure data quality and compliance with regulations such as GDPR, CCPA, etc.
- **Key Aspects of Data Governance**:
