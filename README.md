# 🚀 Employee Data Pipeline (End-to-End Data Engineering Project)

## 📌 Overview
This project demonstrates an **end-to-end data pipeline** built using modern data engineering tools.  
It simulates transactional data, processes it using Spark, stores it in Delta Lake, and loads it into ScyllaDB — all orchestrated using Apache Airflow.

---

## 🏗️ Architecture Diagram

This project follows a modern data engineering pipeline architecture:

- Data is generated using Python (Faker)
- Apache Airflow orchestrates the workflow
- Apache Spark processes the data
- Delta Lake stores the data with versioning
- ScyllaDB serves as the final NoSQL database

<p align="center">
  <img src="images/architecture.png" width="700"/>
</p>

> Airflow orchestrates Spark jobs to process Delta Lake data and load aggregated results into ScyllaDB.

Data Generation (Python + Faker)
↓
Apache Airflow (Orchestration)
↓
Apache Spark (Processing)
↓
Delta Lake (Data Lake Storage)
↓
ScyllaDB (NoSQL Database)

---

## ⚙️ Tech Stack

- **Python** – Data generation
- **Apache Airflow** – Workflow orchestration
- **Apache Spark** – Distributed data processing
- **Delta Lake** – Data lake storage
- **ScyllaDB** – NoSQL database
- **Docker** – Containerization

---

## 🔄 Pipeline Flow

```text
generate_data → load_delta → transform → load_scylla

1️⃣ generate_data
Generates synthetic transaction data using Faker
Introduces duplicates and invalid values for testing
Outputs CSV file
2️⃣ load_delta
Loads raw data into Delta Lake using Spark
3️⃣ transform
Cleans data:
Removes duplicates
Filters invalid transactions
Aggregates customer spending
4️⃣ load_scylla
Loads transformed data into ScyllaDB

📂 Project Structure

assignment-2/
│
├── dags/
│   ├── data_pipeline_dag.py
│   ├── scripts/
│   │   └── generate_data.py
│   └── data/
│       └── transactions_raw.csv
│
├── docker-compose.yml
├── load_to_delta.py
├── transform_data.py
├── load_to_scylla.py
└── README.md

🐳 Setup Instructions

1️⃣ Clone Repository
git clone <your-repo-link>
cd assignment-2

2️⃣ Start Docker Services
docker-compose up -d

3️⃣ Initialize Airflow Database
docker-compose run airflow-webserver airflow db init

4️⃣ Create Admin User
docker-compose run airflow-webserver airflow users create \
    --username admin \
    --password admin \
    --role Admin \
    --email admin@example.com

5️⃣ Access Airflow UI
http://localhost:8081

Login:
Username: admin
Password: admin

6️⃣ Run Pipeline
Open DAG: employee_data_pipeline_v2
Click Trigger DAG
Monitor execution in Graph view

📊 Output
Raw data stored in:

/opt/airflow/dags/data/transactions_raw.csv
Processed data stored in:
Delta Lake
ScyllaDB

⚠️ Challenges & Solutions
🔴 1. Airflow Containers Restarting
Issue: Containers kept stopping
Fix: Initialized Airflow DB using airflow db init
🔴 2. Missing Python Packages
Issue: ModuleNotFoundError: pandas, faker
Fix: Installed packages inside containers
🔴 3. File Path Errors
Issue: Cannot save file into non-existent directory

Fix: Used absolute container path:

/opt/airflow/dags/data/
🔴 4. Docker vs Local Environment Confusion
Issue: Local installations not reflected in containers
Fix: Installed dependencies within Docker environment
🔴 5. Logs Not Accessible (403 Error)
Fix: Ensured consistent secret_key configuration

🧠 Key Learnings
Containers are isolated environments
Airflow requires database initialization before execution
Always use absolute paths in Docker-based systems
Logs are critical for debugging distributed pipelines
Dependency management differs in containerized setups

🎯 Key Highlights
Built a fully containerized data pipeline
Implemented batch processing with Spark
Integrated Delta Lake + ScyllaDB
Orchestrated workflows using Airflow DAGs
Solved real-world issues in distributed systems

📌 Future Improvements
Add CI/CD pipeline
Use Kubernetes for orchestration
Implement real-time streaming (Kafka)
Add monitoring (Prometheus + Grafana)


👨‍💻 Author
Siddesh Yerawar
