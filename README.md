# HappyBooking – Microsoft Fabric Data Engineering Project

A full medallion architecture (Bronze → Silver → Gold) data pipeline built on
Microsoft Fabric, combining batch ingestion (Kaggle CSV), real-time streaming
(Docker → Kafka-compatible Eventstream), and API enrichment (weather + currency),
with dbt-managed dimensional modeling and a Power BI dashboard.

Built as interview portfolio material to demonstrate hands-on data engineering
across ingestion patterns, data quality handling, and analytics delivery.

## Architecture

Docker Stream Producer (Python + Kafka client)
|
v
Fabric Eventstream (Kafka-compatible) ---> Bronze (streaming)
|
Batch CSV (Kaggle) -------------------------> Bronze (batch)
|
Weather API (Open-Meteo) + Currency API (Frankfurter) --> Bronze (enrichment)
|
v
Silver Layer (PySpark: cleaning, type casting, cross-field validation, union)
|
v
Gold Layer (dbt models: dim_city, fact_booking, kpi_revenue)
|
v
Power BI Dashboard (Direct Lake)

## Data Sources

- **Batch**: Kaggle hotel booking dataset, split 70/30 into batch/stream simulation
- **Streaming**: Python producer publishing to Fabric Eventstream via the Kafka protocol, running in Docker
- **API enrichment**: Open-Meteo (weather, geocoding) and Frankfurter (currency exchange rates), both free and key-free

## Tech Stack

Microsoft Fabric (Lakehouse, Notebook, Eventstream, SQL Analytics Endpoint),
PySpark, dbt (dbt-core + dbt-fabricspark via Livy), Docker, Kafka protocol,
Power BI (Direct Lake), Python, Git/GitHub.

## Repository Structure

repo-root/
├── data/ # sample data (full CSVs are gitignored)
├── docker/ # stream producer + Dockerfile
├── notebooks/ # exported Fabric notebooks (Bronze, Silver)
├── dbt_project/happybooking_gold/ # Gold layer dbt models + tests
├── .env.example # required environment variables (no secrets)
└── README.md

## Pipeline Walkthrough

### 1. Bronze Layer

- **Batch**: 1,050,638 rows ingested from CSV via PySpark, with explicit
  `multiLine`/`escape` handling for embedded newlines in free-text fields.
- **Streaming**: A Python producer (`docker/stream_producer.py`) reads rows and
  publishes JSON events to a Fabric Eventstream custom endpoint over the Kafka
  protocol (SASL_SSL auth against Event Hub's Kafka-compatible surface).
- **Enrichment**: Weather data for 19/20 sampled cities (geocoded via Open-Meteo)
  and 29 currency exchange rates (via Frankfurter), each with retry logic for
  transient API failures.

### 2. Silver Layer

- Placeholder cleaning across all 72 string columns (`???`, `___`, stray `!!`/`...` noise → NULL)
- Type casting with domain-specific valid ranges (e.g. star_rating 1–5, nights 1–90),
  including recovery of spelled-out numbers (`"Five"` → `5`)
- Cross-field date validation (checkin should not precede booking date), surfaced
  as a flag column rather than silently nulling suspicious rows
- Deduplication on the natural business key (`booking_id`) rather than a full-row
  hash, after an initial full-column approach proved impractical at scale
- Wide free-text columns (review text/title) split into a separate narrow table
  to keep the main analytical table lean
- Batch and streaming sources combined via `unionByName` with an explicit schema
  assertion beforehand

**Result**: ~73.6% of records pass both key-validity and date-logic checks cleanly;
the remainder are flagged, not discarded, so downstream consumers can decide how
to handle them.

### 3. Gold Layer (dbt)

- `dim_city` — one row per city, enriched with weather data
- `fact_booking` — one row per booking, joined to `dim_city`
- `kpi_revenue` — city-level revenue, cancellation rate, and data-quality rollups

8 dbt tests (uniqueness, not-null, referential integrity) all passing. Two real
bugs were caught and fixed via these tests during development: a dimension table
deduplicating on the wrong grain (causing a join fan-out that nearly doubled the
fact table), and duplicate `booking_id`s surviving a batch/stream union due to
overlapping sample data between the two ingestion paths.

### 4. Power BI Dashboard

Built on a Direct Lake semantic model connected to the three Gold tables:
revenue and booking-count KPI cards, a city-level revenue breakdown, a booking
trend line chart, and a data-quality summary card surfacing how many records
carry flagged/invalid keys.

## Known Limitations / Future Work

- Orchestration (Fabric Data Pipeline with a schedule) not yet implemented —
  notebooks and dbt models are currently run manually
- GitHub Actions CI/CD (PR-triggered tests) not yet implemented
- A handful of cosmetic data quality issues (e.g. hyphenated city name casing)
  were documented but deliberately not fixed, as lower priority than structural
  issues
- Gold layer capacity/performance was constrained by Fabric trial-tier compute;
  a production capacity would substantially reduce write times observed during
  Silver-layer processing

## Setup

1. Copy `.env.example` to `.env` and fill in your Eventstream connection details
2. `pip install -r docker/requirements.txt` (or use the provided Dockerfile)
3. See `dbt_project/happybooking_gold/` for dbt setup against Fabric via Livy
