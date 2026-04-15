# BIDs PDF ETL: Airflow + Docker + PostgreSQL

End-to-end data pipeline that extracts structured data from PDF documents and loads it into PostgreSQL, orchestrated with Apache Airflow.

---

## Overview

This project processes unstructured PDF files (e.g., award letters, bid tabs) and converts them into structured datasets ready for analysis.

#### Flow
```
PDF files → Extractors → Transformer → PostgreSQL
                    ↓
                 Airflow (orchestration)
```

#### Architecture

```
┌──────────────────────────────────────────────┐
│                 Airflow DAG                  │
│          (Orchestration Layer)               │
│                                              │
│   ┌──────────────┐                           │
│   │  Extractors  │                           │
│   │ (PDF Parsing)│                           │
│   └──────┬───────┘                           │
│          │                                   │
│          ▼                                   │
│   ┌──────────────┐                           │
│   │ Transformer  │                           │
│   │ (Normalize)  │                           │
│   └──────┬───────┘                           │
│          │                                   │
│          ▼                                   │
│   ┌──────────────┐                           │
│   │   Loader     │                           │
│   │ PostgreSQL   │                           │
│   └──────────────┘                           │
│                                              │
└──────────────────────────────────────────────┘
                  ▲
                  │
        ┌──────────────────┐
        │    PDF Files     │
        │  (Batch Input)   │
        └──────────────────┘
```

---

## Project Structure

```
etl/
├── dags/
│   └── bids_pdf_etl.py
├── extractors/
│   ├── award_letter_extractor.py
│   ├── base_extractor.py
│   ├── bid_tabs_extractor.py
|   ├── bid_tabs_idiq_extractor.py
|   ├── bids_as_read_extractor.py 
│   ├── invitation_to_bid_extractor.py
│   └── item_c_report_extractor.py
├── inbox/
├── loaders/
│   └── bid_line_items_loader.py
├── raw_json/
├── transformers/
│   └── transform.py
├── db/
│   ├── postgresql_client.py
│   └── init.sql
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
└── .env
```

---

## Quick Start

### 1. Generate secrets

```bash
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

Add to `.env`:

```
AIRFLOW_FERNET_KEY=<generated_key>
AIRFLOW_SECRET_KEY=<random_string>
```

---

### 2. Build and start services

```bash
docker compose up --build -d
```

---

### 3. Configure Airflow connection

```bash
docker compose exec airflow-scheduler \
  airflow connections add fs_default \
    --conn-type fs \
    --conn-extra '{"path": "/"}'
```

---

### 4. Access Airflow UI

http://localhost:8080

**Login**:
user: `admin`
pass: `admin`

---

## Input Data (PDF Ingestion)

The pipeline scans the `inbox/` directory every 5 minutes for new batches.

### Batch structure

Each batch is a flat folder:

```
inbox/
└── 2024-01-15_batch/
    ├── DA00592 Award Letter.pdf
    ├── DA00592 Bid Tabs.pdf
    ├── L231206A Item C Report.pdf
    └── Invitation to Bid.pdf
```

In order to trigering data procesing place PDF batches inside:

```
inbox/
```

Example:

```
inbox/2024-01-15_batch/
```

---

## Querying Data

```bash
docker compose exec postgres psql -U etl -d etl
```
Example:
```sql
-- Count loaded rows
SELECT proposals_contract_id, COUNT(*)
FROM ncdot.bid_line_items
GROUP BY 1;
```

## Stopping the Environment
```bash
docker compose down # stop containers
docker compose down -v # stop + delete all data
```

---

## Features

- Modular extractors
- Automatic contract detection
- Airflow orchestration
- Dockerized environment
- PostgreSQL storage
- Idempotent processing
