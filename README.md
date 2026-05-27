# 🏥 Healthcare Claims ETL Pipeline

> End-to-end data engineering pipeline for ingesting, transforming, and modeling 1M+ CMS Medicare claims records — built to surface actionable cost and utilization insights across providers.

[![Python](https://img.shields.io/badge/Python-3.11-blue?logo=python)](https://python.org)
[![Apache Airflow](https://img.shields.io/badge/Airflow-2.8-017CEE?logo=apache-airflow)](https://airflow.apache.org)
[![dbt](https://img.shields.io/badge/dbt-1.7-FF694B?logo=dbt)](https://getdbt.com)
[![AWS](https://img.shields.io/badge/AWS-S3%20%2B%20Redshift-FF9900?logo=amazon-aws)](https://aws.amazon.com)
[![License: MIT](https://img.shields.io/badge/License-MIT-green)](LICENSE)

---

## 📌 Overview

Healthcare providers lack a reliable way to analyze claims patterns at scale. This pipeline automates ingestion and modeling of **CMS Medicare Part A & B** public data, enabling:

- Provider-level cost and utilization benchmarking
- Claims anomaly detection (over-billing, outlier procedures)
- Downstream ML-ready feature datasets for predictive modeling

**Dataset:** [CMS Medicare Provider Utilization & Payment Data](https://data.cms.gov/provider-summary-by-type-of-service)

---

## 🏗️ Architecture

```
CMS Public API
     │
     ▼
┌─────────────────┐
│  Apache Airflow  │  ← Orchestration (daily DAG)
│  (ingestion.py)  │
└────────┬────────┘
         │  Raw JSON / CSV
         ▼
┌─────────────────┐
│    AWS S3        │  ← Raw Data Lake (partitioned by year/month)
│  (raw/ layer)   │
└────────┬────────┘
         │  COPY command
         ▼
┌─────────────────┐
│  AWS Redshift    │  ← Analytical Warehouse
│  (raw schema)   │
└────────┬────────┘
         │
         ▼
┌─────────────────────────────────────┐
│              dbt Project             │
│  staging → intermediate → marts     │
│  + schema tests + data quality       │
└────────┬────────────────────────────┘
         │
         ▼
┌─────────────────┐
│  Analytics /    │  ← BI tools, ML feature prep
│  ML Features    │
└─────────────────┘
```

---

## 📁 Project Structure

```
healthcare-claims-pipeline/
│
├── airflow/
│   ├── dags/
│   │   └── cms_ingestion_dag.py          # Main daily DAG
│   └── plugins/
│       └── cms_api_hook.py               # Custom CMS API hook
│
├── ingestion/
│   ├── cms_api_client.py                 # CMS API pagination + retry logic
│   ├── s3_uploader.py                    # Raw file upload to S3
│   └── schema_validator.py              # Schema-drift detection on raw data
│
├── dbt/
│   ├── models/
│   │   ├── staging/
│   │   │   ├── stg_claims_partA.sql      # Part A claims staging
│   │   │   ├── stg_claims_partB.sql      # Part B claims staging
│   │   │   └── stg_providers.sql         # Provider reference data
│   │   ├── intermediate/
│   │   │   ├── int_claims_joined.sql     # Join claims + provider context
│   │   │   └── int_procedure_rollup.sql  # Procedure-level aggregations
│   │   └── marts/
│   │       ├── mart_provider_utilization.sql   # Provider cost/volume metrics
│   │       ├── mart_procedure_benchmarks.sql   # Procedure-level benchmarks
│   │       └── mart_ml_features.sql            # ML-ready feature table
│   ├── tests/
│   │   ├── assert_no_null_claim_ids.sql
│   │   ├── assert_row_count_threshold.sql
│   │   └── assert_valid_provider_npi.sql
│   ├── macros/
│   │   └── generate_surrogate_key.sql
│   ├── dbt_project.yml
│   └── profiles.yml.example
│
├── sql/
│   ├── ddl/
│   │   ├── raw_claims_partA.sql          # Raw table DDL
│   │   └── raw_claims_partB.sql
│   └── analysis/
│       ├── top_procedures_by_cost.sql    # Ad-hoc analysis queries
│       └── provider_outlier_detection.sql
│
├── notebooks/
│   └── eda_claims_exploration.ipynb      # Exploratory analysis
│
├── tests/
│   ├── test_cms_api_client.py            # Unit tests for ingestion
│   ├── test_schema_validator.py
│   └── conftest.py
│
├── docs/
│   ├── data_dictionary.md                # Field definitions for all marts
│   ├── pipeline_runbook.md               # How to run + troubleshoot
│   └── architecture_diagram.png
│
├── .github/
│   └── workflows/
│       └── ci.yml                        # GitHub Actions: lint + dbt compile
│
├── requirements.txt
├── docker-compose.yml                    # Local Airflow dev environment
├── .env.example
└── README.md
```

---

## 🚀 Getting Started

### Prerequisites

- Python 3.11+
- Docker & Docker Compose (for local Airflow)
- AWS account with S3 + Redshift access
- dbt-redshift adapter

### 1. Clone & install

```bash
git clone https://github.com/yourusername/healthcare-claims-pipeline.git
cd healthcare-claims-pipeline
pip install -r requirements.txt
```

### 2. Configure environment

```bash
cp .env.example .env
# Fill in: AWS credentials, Redshift connection, CMS API key
```

### 3. Start local Airflow

```bash
docker-compose up -d
# Visit http://localhost:8080 (admin / admin)
```

### 4. Run dbt models

```bash
cd dbt
dbt deps
dbt run --select staging
dbt run --select intermediate
dbt run --select marts
dbt test
```

---

## 📊 Key dbt Models

| Model | Layer | Description |
|---|---|---|
| `stg_claims_partA` | Staging | Cleaned inpatient/hospital claims |
| `stg_claims_partB` | Staging | Cleaned outpatient/physician claims |
| `int_claims_joined` | Intermediate | Claims enriched with provider attributes |
| `mart_provider_utilization` | Mart | Provider-level cost, volume, avg payment |
| `mart_procedure_benchmarks` | Mart | National/regional benchmarks per HCPCS code |
| `mart_ml_features` | Mart | Feature-engineered table for predictive models |

---

## ✅ Data Quality

All mart models are covered by dbt tests:

- **Schema tests:** `not_null`, `unique`, `accepted_values` on all primary keys and critical fields
- **Custom tests:** row-count thresholds, NPI validity checks, payment range assertions
- **Freshness checks:** source freshness assertions on raw CMS data (expected daily)

---

## 🤖 ML Feature Output

`mart_ml_features` produces a flat, analysis-ready table for downstream tasks such as:

- **Cost prediction:** predict expected payment per procedure per provider
- **Anomaly detection:** flag providers with utilization patterns outside 2σ
- **Readmission risk scoring** (extensible with patient-level data)

Features include: provider NPI, specialty, state, procedure volume, avg allowed amount, stddev payment, rank within specialty, year-over-year change metrics.

---

## 🧪 Running Tests

```bash
# Python unit tests
pytest tests/ -v

# dbt tests
cd dbt && dbt test

# CI runs automatically on pull requests via GitHub Actions
```

---

## 📖 Documentation

See [`docs/data_dictionary.md`](docs/data_dictionary.md) for full field definitions and [`docs/pipeline_runbook.md`](docs/pipeline_runbook.md) for operational guidance.

---

## 📄 Data Source & License

Data sourced from [CMS.gov](https://data.cms.gov) — publicly available under CMS data use terms.  
Code: MIT License. See [LICENSE](LICENSE).
