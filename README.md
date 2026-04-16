# BIDs PDF ETL: Airflow + Docker + PostgreSQL

End-to-end data pipeline that extracts structured data from PDF documents and loads it into PostgreSQL, orchestrated with Apache Airflow.

---

## Overview

This project solves the challenge of transforming unstructured PDF documents into structured, queryable data.

It processes multiple document types (Award Letters, Bid Tabs, etc.), extracts relevant fields, normalizes them, and loads them into a relational database.


### 0. Output Data

As part of this proof of concept, the processed data is made available as a CSV file located in the root directory of the project. 

### 1. Flow
```
PDF Batch (inbox/)
        ↓
Airflow DAG triggered (every 5 min)
        ↓
Extraction (per file type)
        ↓
Raw JSON (intermediate layer)
        ↓
Transformation (normalize schema)
        ↓
Load into PostgreSQL
```

#### Batch Processing
- Files are grouped by batch folder
- Each batch is processed atomically
- A .processed marker prevents reprocessing (idempotency)

#### Handling New / Updated Data
- New files → automatically detected via folder polling
- Already processed batches → skipped using marker files

#### Smart Detection
- Contract ID extracted from filename patterns
- Document type inferred via keywords

### 2. Architecture

The solution is built around Apache Airflow as the orchestration layer, which manages all ETL steps (extraction, transformation, and loading).

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
### 3. Design Decisions
- **`Airflow`** → central orchestration, scheduling, and retry management.
- **`Modular extractors`** → isolate parsing logic per document type.
- **`Transformation layer`** → ensures consistent schema across heterogeneous PDFs.
- **`PostgreSQL`** → structured storage for analytical queries.
- **`Docker`** → reproducible environment and easy 
  
This design ensures extensibility, maintainability, and separation of concerns.

---

### 4. Project Structure

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
