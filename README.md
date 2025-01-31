# Modern Data Pipeline with Airbyte, dbt (Cosmos), and Airflow

ETL pipeline integrating:
- **Airbyte** for ingesting data from HTTP source to BigQuery
- **dbt + Cosmos** for data transformations into data marts
- **Airflow** as the orchestration engine

## Architecture
[HTTP API] → [Airbyte] → [BigQuery Raw] → [dbt + Cosmos] → [BigQuery Data Mart] → [Airflow DAG with Cosmos Task Group]

## Prerequisites
- Google Cloud Project with BigQuery
- BigQuery Service Account Key (JSON)
- Python 3.8+
- Docker & Docker Compose
- Airbyte Open Source
- dbt Core 1.5+
- Apache Airflow 2.6+
- Cosmos 1.2+
