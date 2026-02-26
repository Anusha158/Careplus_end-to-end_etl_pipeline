# CarePlus End-to-End ETL Pipeline

## 📌 Project Overview

Built an end-to-end Data Engineering pipeline for a BPO organization that manages customer support operations for enterprise clients (e.g., retail companies like Walmart).

The goal was to centralize structured and unstructured ticketing data into a **Single Source of Truth**, enabling performance monitoring, system diagnostics, and operational analytics.

---

## 🏢 Business Context

The BPO company handles customer care operations for multiple enterprise clients.

Support workflow:

Customer Issue → Ticket Created → Assigned to Agent → Resolution Logged → System Logs Generated

Data Sources Included:
- Structured ticket metadata (ticket_id, agent_id, status, timestamps)
- Unstructured log files (system logs, error traces, performance logs)
- Agent activity data
- Operational metrics

---

## 🎯 Problem Statement

- Ticket data was fragmented across multiple systems.
- Log files were unstructured and difficult to analyze.
- Performance monitoring required manual effort.
- No centralized reporting system existed.

This created:
- Delayed issue resolution
- Poor visibility into system performance
- Inconsistent reporting metrics

---

## 🏗️ Solution Architecture

Designed a layered ETL pipeline:

This repository contains an end-to-end ETL pipeline for CarePlus support data. It includes components
for data ingestion, transformation, and data warehousing/analytics with clear config and deployment
artifacts so teams can reproduce and extend the pipeline.

**Quick Links**
- Project root: this `README`
- Configs: `config/config_dev.yaml`, `config/config_prod.yaml`
- Source: `src/` (ingestion, transformation, warehousing & analytics)

## Architecture Overview

The pipeline follows a classic ETL architecture:
- Data Ingestion: pull/collect raw support logs and tickets, stage them on S3.
- Data Transformation: clean, normalize and convert raw data into analytical parquet tables(using lambda and Glue)
- Data Warehousing & Analytics: load transformed data into analytic stores (Redshift/Athena), and run queries/dashboards

## Getting Started (local)

1. Create a Python environment (recommended):

```powershell
python -m venv .venv
.\\.venv\\Scripts\\Activate.ps1
pip install -r requirements.txt
```

2. Inspect and copy environment samples in `src/*/sample.env` and set credentials/paths.
3. Update `config/config_dev.yaml` with local paths or S3 endpoints as needed.
4. Run ingestion notebooks in `src/data-ingestion/` to collect raw data, or run any provided scripts.

## Running Transformations

- Use the notebooks in `src/data-transformation/` for interactive runs and iterative development.
- For scripted transformations, see `src/data-transformation/support-tickets-transformation/glue-transformation/automate_etl_support_tickets.py`.

## Analytics & Warehousing

- SQL queries for analytics and table creation are in `src/data-warehousing-analytics/athena-sql-queries/` and `src/data-warehousing-analytics/redshift-setup/`.
- Use the notebooks in `src/data-warehousing-analytics/` to validate loads and inspect parquet outputs.

## Configuration

- `config/config_dev.yaml` — development settings (local paths, test credentials)
- `config/config_prod.yaml` — production settings (S3 buckets, Redshift endpoints)

Always avoid committing real credentials. Use the sample env files and environment variables for secrets.

