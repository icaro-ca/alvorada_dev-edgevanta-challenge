-- ---------------------------------------------------------------------------
-- init.sql
-- Runs once on first postgres container start.
-- Creates:
--   • airflow  database + user  (Airflow metadata)
--   • etl      database + user  (ETL target)
-- ---------------------------------------------------------------------------

-- Airflow metadata DB
CREATE USER airflow WITH PASSWORD 'airflow';
CREATE DATABASE airflow OWNER airflow;

-- ETL target DB
CREATE USER etl WITH PASSWORD 'etl';
CREATE DATABASE etl OWNER etl;

-- Switch to ETL database to create schema and table
\connect etl etl

CREATE SCHEMA IF NOT EXISTS ncdot;

CREATE TABLE IF NOT EXISTS ncdot.bid_line_items (
    -- identity
    id                            BIGSERIAL PRIMARY KEY,
    loaded_at                     TIMESTAMPTZ NOT NULL DEFAULT now(),
    run_id                        TEXT, -- Airflow run_id for traceability
    source_dir                    TEXT, -- inbox subdirectory name

    -- line item
    proposal_items_line_number    TEXT,
    items_number                  TEXT,
    items_category                TEXT,
    items_description             TEXT,
    proposal_items_quantity       TEXT,
    items_unit                    TEXT,
    item_section                  TEXT,

    -- bid
    bids_rank                     TEXT,
    vendors_name                  TEXT,
    bid_items_unit_price          TEXT,
    bid_items_extension           TEXT,

    -- proposal-level fields (denormalised)
    owners_state                  TEXT,
    owners_name                   TEXT,
    proposals_county              TEXT,
    proposals_contract_id         TEXT,
    proposals_project_number      TEXT,
    proposals_description         TEXT,
    lettings_date                 TEXT,
    proposals_completion_date     TEXT,
    bids_value                    TEXT,
    proposals_district_name       TEXT,
    proposals_call_number         TEXT,
    proposals_project_type        TEXT,
    proposals_cost_estimate       TEXT
);

-- Index for the most common lookup pattern
CREATE INDEX IF NOT EXISTS idx_bid_line_items_contract
    ON ncdot.bid_line_items (proposals_contract_id);
