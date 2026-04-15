# NC DOT PDF ETL — Docker Compose + Airflow + PostgreSQL

End-to-end pipeline: PDF files → structured rows in PostgreSQL,
orchestrated by Apache Airflow.

---

## Project structure

```
etl/
├── dags/
│   └── ncdot_pdf_etl.py          ← Airflow DAG
├── extractors/                    ← PDF extractors (your existing code)
│   ├── base_extractor.py
│   ├── award_letter_extractor.py
│   ├── bid_tabs_extractor.py
│   ├── invitation_to_bid_extractor.py
│   └── item_c_report_extractor.py
├── transformers/
│   └── transform.py               ← PDFDataTransformer
├── loaders/
│   └── bid_line_items_loader.py   ← Inserts DataFrame into Postgres
├── db/
│   ├── postgresql_client.py       ← PostgreSQL client (env-var config)
│   └── init.sql                   ← Creates airflow + etl databases and schema
├── Dockerfile                     ← Extends official Airflow image
├── docker-compose.yml
├── requirements.txt
└── .env                           ← Secrets (never commit this)
```

---

## Quick start

### 1. Generate secrets

```bash
# Fernet key (required by Airflow to encrypt connection passwords)
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"

# Paste the output into .env:
#   AIRFLOW_FERNET_KEY=<output>
#   AIRFLOW_SECRET_KEY=<any random string>
```

### 2. Build and start

```bash
docker compose up --build -d
```

First boot takes ~2 minutes. The `airflow-init` container runs
`airflow db migrate` and creates the admin user, then exits.

### 3. Add the filesystem connection in Airflow

The `FileSensor` needs a connection named `fs_default` that points to `/`:

```bash
docker compose exec airflow-scheduler \
  airflow connections add fs_default \
    --conn-type fs \
    --conn-extra '{"path": "/"}'
```

### 4. Open the Airflow UI

http://localhost:8080  
Login: `admin` / `admin`

Enable the `ncdot_pdf_etl` DAG from the UI.

---

## Dropping PDFs to process

The pipeline triggers automatically every 5 minutes. It scans the
`inbox/` volume for new **batch subdirectories**.

Each batch directory is a **flat folder** — all contracts mixed together.
Files are grouped into contracts automatically by detecting the contract ID
and document type from the filename using keyword matching. No strict naming
convention is required.

**Contract ID detection** — the first ID-like token found anywhere in the filename:

| Pattern | Example |
|---|---|
| All digits | `12107176_ Bid Tabs.pdf` |
| Letters + digits | `DA00592 Award Letter.pdf` |
| Digits + letters | `L231206A_ Item C Report.pdf` |

**Document type detection** — by keyword scan (case-insensitive):

| Keyword in filename | Mapped type |
|---|---|
| `Award Letter` | award_letter |
| `Bid Tab`, `AWP Bid Tabs` | bid_tabs |
| `Invitation to Bid` | invitation_to_bid |
| `Item C` | item_c_report |

Files with no detectable contract ID (e.g. `Item C Report.pdf`) are automatically assigned to **all contracts** in
the same batch directory.

```bash
# Example: drop a batch with two contracts
mkdir -p inbox/2024-01-15_batch
cp /path/to/*.pdf inbox/2024-01-15_batch/
```

The pipeline picks it up within 5 minutes. After all contracts in the
batch are loaded, a `.processed` sentinel is written so the batch is
never reprocessed.

---

## Querying results

```bash
# Connect to the ETL database (host port 5433 to avoid conflicts with a local Postgres)
docker compose exec postgres psql -U etl -d etl

# Count loaded rows
SELECT proposals_contract_id, COUNT(*) FROM ncdot.bid_line_items GROUP BY 1;
```

---

## Stopping

```bash
docker compose down          # stop containers, keep volumes
docker compose down -v       # stop containers AND delete all data
```
