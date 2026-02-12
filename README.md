# Customer Transactions ETL Pipeline  
**Local → GCS → BigQuery (Raw) → BigQuery (Curated) → Power BI**

---

## Overview

This project implements a cloud-based batch ETL pipeline that ingests customer and transaction data from local CSV files, loads them into Google Cloud Storage (GCS), processes them in BigQuery, and produces curated analytical tables for business reporting in Power BI.

### What This Project Demonstrates

- End-to-end ETL / ELT workflow  
- Cloud storage integration (Google Cloud Storage)  
- BigQuery data warehouse ingestion  
- SQL-based transformations  
- Data modelling (raw → curated layer separation)  
- BI reporting integration (Power BI)

---

## Architecture

Local CSV
↓
Google Cloud Storage (GCS)
↓
BigQuery (raw layer)
↓
BigQuery (curated layer via SQL)
↓
Power BI Dashboard


---


---

## Inputs

### `customers.csv`
Customer master data containing identity and signup information.

### `transactions.csv`
Transaction-level event data including timestamps, amount, and merchant details.

---

## Outputs (BigQuery – Curated Dataset)

### `curated.customer_summary`

Customer-level aggregated metrics:

- Transaction count  
- Total spend  
- Average transaction value  
- First transaction timestamp  
- Last transaction timestamp  
- Activity status flag (`NO_TXNS`, `ACTIVE_30D`, `INACTIVE`)

---

### `curated.daily_kpis`

Daily performance metrics:

- Transaction count  
- Active customers  
- Revenue  
- Average order value  

---

## Repository Structure

```
customer-etl/
│
├── data/
│ ├── customers.csv
│ └── transactions.csv
│
├── sql/
│ ├── 02_transform_customer_summary.sql
│ └── 03_transform_daily_kpis.sql
│
├── src/
│ ├── config.py
│ ├── gcs_upload.py
│ ├── bq_load_raw.py
│ ├── bq_run_sql.py
│ ├── run_pipeline.py
│ └── data_separation.py # optional (if starting from single dataset)
│
├── requirements.txt
└── README.md
```


---

## Setup (One-Time Configuration)

### Create a GCS Bucket

```
gs://customer-etl-bucket-raw/
```


---

### Create BigQuery Datasets

Create two datasets in your project:

- `raw`
- `curated`

---

### Create Service Account & Permissions

Grant the following roles:

- **Storage Object Admin**
- **BigQuery Job User**
- **BigQuery Data Editor**

Download the service account JSON key.

---

### Set Credentials

#### Windows (PowerShell)

```
setx GOOGLE_APPLICATION_CREDENTIALS "C:\path\sa-key.json"
```

---

## Install Dependencies

```
pip install -r requirements.txt
```

---

## Environment Variables

Set the following:

```
GCP_PROJECT_ID=your-gcp-project
GCS_BUCKET=your-customer-etl-bucket
BQ_RAW_DATASET=raw
BQ_CURATED_DATASET=curated
```

## Run the Pipeline

From the project root:

```
python -m src.run_pipeline
```

The Pipeline Performs:

1. Uploads CSV files to GCS
2. Loads raw tables into BigQuery
3. Executes SQL transformation scripts
4. Creates curated analytics tables

---

## Power BI Connection

- Open Power BI Desktop
- Select 
    `Get Data → Google BigQuery`
- Choose dataset: `curated`
- Load:
    `customer_summary`
    `daily_kpis`



