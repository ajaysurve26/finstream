# 🛡️ FinStream — Real-time Financial Transaction Pipeline

A production-grade streaming data engineering project built on Azure, Databricks and dbt.

## Architecture

Fake Financial Data Generator
↓
Azure Event Hubs (Kafka-compatible)
↓
Databricks Structured Streaming
↓
┌─────────────────────────────────┐
│         Delta Lake              │
│  Bronze → Silver → Gold        │
│  (Medallion Architecture)       │
└─────────────────────────────────┘
↓
dbt Transformations + Tests
↓
Great Expectations Quality Gates
↓
Streamlit Live Dashboard

## Tech Stack

| Layer | Technology |
|---|---|
| Ingestion | Azure Event Hubs |
| Storage | Azure Data Lake Gen2 |
| Compute | Azure Databricks |
| Table Format | Delta Lake |
| Transformation | dbt-databricks |
| Data Quality | Great Expectations |
| Dashboard | Streamlit + Plotly |
| Infrastructure | Terraform |
| CI/CD | GitHub Actions |

## Key Features

- Real-time streaming with exactly-once semantics
- Medallion architecture (Bronze → Silver → Gold)
- Rule-based fraud detection engine
- Automated data quality validation (20 checks)
- User risk scoring system
- Live fraud intelligence dashboard
- Full CI/CD pipeline with automated testing
- Infrastructure as Code with Terraform

## Pipeline Stages

### Phase 1 — Infrastructure
Terraform provisions all Azure resources — Resource Group, ADLS Gen2, Event Hubs, Databricks workspace.

### Phase 2 — Data Generator
Python script using Faker generates realistic financial transactions with rule-based fraud detection and publishes to Event Hubs every 2 seconds.

### Phase 3 — Bronze Layer
Databricks Structured Streaming reads from Event Hubs via Kafka protocol, appends raw JSON with metadata columns to Bronze Delta table. Checkpoint-based exactly-once delivery.

### Phase 4 — Silver Layer
dbt models parse JSON, deduplicate records, apply type casting, add derived columns and data quality flags. 8 automated dbt tests validate the output.

### Phase 5 — Gold Layer
Three aggregated Gold tables — hourly transaction summary, user risk scores, fraud by country — serve the dashboard with pre-computed metrics.

### Phase 6 — Dashboard
Streamlit dashboard with live KPIs, fraud alerts, transaction volume charts, country risk analysis, merchant bubble chart and top risky users table. Auto-refreshes every 30 seconds.

### Phase 7 — Data Quality
Great Expectations validation suite with 20 checks across completeness, uniqueness, validity, consistency and volume categories. Acts as a quality gate between Silver and Gold.

### Phase 8 — CI/CD
GitHub Actions pipeline runs on every push — SQL linting, dbt model runs, dbt tests, data quality validation and dbt docs generation.

## How to Run

### Prerequisites
- Azure subscription
- Python 3.11+
- Terraform 1.x

### Setup
```bash
git clone https://github.com/ajaysurve26/finstream.git
cd finstream
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### Infrastructure
```bash
cd terraform
terraform init
terraform apply
```

### Data Generator
```bash
cd data_generator
python3 generator.py
```

### Dashboard
```bash
cd dashboard
streamlit run app.py
```

## CI/CD Pipeline

Every push to main triggers:
1. SQL linting with SQLFluff
2. dbt model runs against Databricks
3. dbt tests (uniqueness, nulls, accepted values)
4. Great Expectations validation (20 checks)
5. dbt docs generation

## Interview Notes

This project demonstrates:
- Streaming pipeline design with exactly-once semantics
- Medallion architecture implementation
- dbt best practices (tests, docs, incremental models)
- Data quality engineering
- Infrastructure as Code
- CI/CD for data pipelines
- Azure cloud services
- Delta Lake (ACID, time travel, schema evolution)