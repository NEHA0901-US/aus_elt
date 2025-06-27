# Australian Company Data Pipeline

## Overview

This project implements an end-to-end data pipeline to ingest, process, and model Australian company data from two primary sources:
- **Common Crawl**: Extracts .au domain company web presence and metadata.
- **Australian Business Register (ABR)**: Extracts official business entity details from government-published XML datasets.

The pipeline leverages PySpark for scalable ETL, PostgreSQL for raw data storage, and dbt for data modeling and analytics.

---

## Pipeline Architecture

![image](https://github.com/user-attachments/assets/88890fe1-a7cb-4d56-b863-fca5f1a5d164)


### Description

1. **Ingestion**:  
   - Fetches WAT records from Common Crawl and parses ABR XMLs from a government ZIP file.
   - Both are loaded into Spark DataFrames.

2. **Raw Storage**:  
   - DataFrames are written to PostgreSQL tables (`core_db.commoncrawl_companies`, `core_db.abr_entities`).

3. **Modeling (dbt)**:  
   - Raw tables are modeled in dbt through raw, staging (validation/cleaning), and production layers.
   - Final models include a dimension table (`dim_companies`) and a fact table (`fact_company_presence`).

---

## Database Schema (PostgreSQL DDL)

### Raw Tables

```sql
-- core_db.commoncrawl_companies
CREATE TABLE core_db.commoncrawl_companies (
    url TEXT,
    company_name TEXT,
    industry TEXT,
    ingested_at TIMESTAMP
);

-- core_db.abr_entities
CREATE TABLE core_db.abr_entities (
    abn TEXT,
    entity_name TEXT,
    entity_type TEXT,
    entity_status TEXT,
    entity_address TEXT,
    entity_postcode TEXT,
    entity_state TEXT,
    entity_start_date TEXT,
    ingested_at TIMESTAMP
);
```
### Stage Tables
```sql
CREATE TABLE IF NOT EXISTS core_db.stg_commoncrawl_companies (
    auto_id BIGSERIAL PRIMARY KEY,
    url TEXT NOT NULL,
    company_name TEXT NOT NULL,
    industry TEXT,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    -- generated columns for normalized values
    url_normalized TEXT GENERATED ALWAYS AS (LOWER(TRIM(url))) STORED,
    company_name_normalized TEXT GENERATED ALWAYS AS (LOWER(TRIM(company_name))) STORED,

    -- unique constraint on normalized values
    UNIQUE (url_normalized, company_name_normalized)
);

CREATE TABLE IF NOT EXISTS core_db.stg_abr_entities (
    auto_id BIGSERIAL PRIMARY KEY,
    abn TEXT NOT NULL,
    entity_name TEXT NOT NULL,
    entity_type TEXT,
    entity_status TEXT CHECK (entity_status IN ('ACT', 'CAN')),
    entity_address VARCHAR,
    entity_postcode INTEGER CHECK (char_length(entity_postcode::text) = 4),
    entity_state TEXT,
    entity_start_date DATE NOT NULL CHECK (entity_start_date <= CURRENT_DATE),
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    needs_verification BOOLEAN DEFAULT FALSE,

    -- generated columns for normalized values
    entity_name_normalized TEXT GENERATED ALWAYS AS (LOWER(TRIM(entity_name))) STORED,

    -- unique constraint for deduplication
    UNIQUE (abn, entity_name_normalized)
);
```

### Production Models

```sql
-- dim_companies
CREATE TABLE dim_companies (
    abn TEXT,
    entity_name TEXT,
    entity_type TEXT,
    entity_status TEXT,
    entity_address TEXT,
    entity_postcode TEXT,
    entity_state TEXT,
    entity_start_date TEXT,
    abr_ingested_at TIMESTAMP,
    url TEXT,
    industry TEXT,
    web_ingested_at TIMESTAMP
);

-- fact_company_presence
CREATE TABLE fact_company_presence (
    abn TEXT,
    url TEXT,
    entity_name TEXT,
    industry TEXT,
    abr_ingested_at TIMESTAMP,
    web_ingested_at TIMESTAMP
);
```
Link to the DDL design -> 
---

## Technology Stack Justification

- **PySpark**: Handles large-scale data processing and transformation efficiently.
- **PostgreSQL**: Reliable, open-source RDBMS for storing raw and processed data.
- **dbt**: Enables modular, testable, and version-controlled data modeling and transformation.
- **Python**: Rich ecosystem for data engineering, with libraries for web, XML, and data processing.

---

## Setup & Running Instructions

### Prerequisites

- Python 3.11+
- Java 8+ (for Spark)
- PostgreSQL 13+ (running locally or accessible remotely)
- [dbt](https://docs.getdbt.com/docs/installation) (for data modeling)

### 1. Clone the Repository

```bash
git clone <repo-url>
cd aus_elt
```

### 2. Install Python Dependencies

Using pip:
```bash
pip install -r requirements.txt
pip install pyspark
```
Or with pipenv:
```bash
pipenv install
```

### 3. Configure PostgreSQL

- Ensure PostgreSQL is running and accessible.
- Default connection (see `container/steps/db_conn.py`):
  ```
  url = "jdbc:postgresql://localhost:5432/aus_comp_db"
  user = "postgres"
  password = "StrongPass123!"
  schema = "core_db"
  ```
- Create the database and schema if not present:
  ```sql
  CREATE DATABASE aus_comp_db;
  CREATE SCHEMA core_db;
  ```

### 4. Run the ETL Pipeline

```bash
python container/execution/aus_comp_pipeline/full_pipeline.py
```

### 5. Run dbt Models

```bash
cd dbt/aus_comp
dbt run
dbt test
```

---

## Project Structure

```
aus_elt/
│
├── container/
│   ├── steps/         # ETL step modules (ingest, upsert, db_conn)
│   └── execution/     # Pipeline execution scripts for each ingests
|   |__ Program.py     # Driver program
│
├── dbt/
│   └── aus_comp/      # dbt project for modeling and analytics
│
├── requirements.txt   # Python dependencies
├── Pipfile            # Python environment (pipenv)
└── README.md
```

---

## Contact

For questions or contributions, please open an issue or contact the maintainer.
