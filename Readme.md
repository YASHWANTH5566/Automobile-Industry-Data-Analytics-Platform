# 🚗 Automobile Industry Data Analytics Platform

An end-to-end **Lakehouse Medallion Architecture** pipeline built on **Databricks**, covering ingestion, streaming transformations, dimensional modeling, and AI-generated business reporting — delivered straight to Slack every day.

---

## 📌 Overview

This platform ingests raw automobile industry data (sales, production, service, warranty, inventory, dealer and customer master data) and transforms it through **Bronze → Silver → Gold** layers using **Delta Live Tables (DLT)**. The Gold layer powers live dashboards, while a **Groq Llama-powered LLM job** reads the final aggregated KPIs every day and posts a plain-English business summary — complete with trend comparisons and anomaly detection — directly to **Slack**.

> Built to simulate a real automotive OEM/dealer network analytics stack: 10 source domains, 25 upstream tables, 15+ downstream gold/dashboard tables, all orchestrated as a single Databricks Job.

---

## 🏗️ Architecture

```
                 ┌──────────────┐
   Raw CSVs  ──▶ │   BRONZE     │  Auto Loader (cloudFiles) + Watermarking + CDC hash
 (S3 landing)    └──────┬───────┘
                        │
                        ▼
                 ┌──────────────┐
                 │   SILVER     │  Cleansing • PII Masking • Dedup • SCD Type 2
                 │              │  Enrichment via joins across domains
                 └──────┬───────┘
                        │
                        ▼
                 ┌──────────────┐
                 │    GOLD      │  KPI aggregates • Anomaly detection (z-score)
                 │              │  Daily trend comparison tables
                 └──────┬───────┘
                        │
            ┌───────────┴────────────┐
            ▼                        ▼
   📊 BI Dashboards          🤖 LLM Daily Summary (Groq Llama)
                                       │
                                       ▼
                                  💬 Slack Channel
```

**Orchestration:** One Databricks Job chains the DLT pipeline → Gold KPI computation → LLM summary generation → Slack delivery, fully automated end to end.

---

## 🧱 Medallion Layers

### 🥉 Bronze — Raw Ingestion
- Streaming ingestion via **Auto Loader** (`cloudFiles`) from S3 landing zones across **10 source domains**: customer, dealer, dealer_parts, inventory, parts, production, sales, service, vehicle_master, warranty
- **Watermarking** applied on event-time columns (`sale_date`, `production_date`, `service_date`, `claim_date`) to bound state and handle late-arriving data — 3-day threshold for transactional tables, 7-day for master data
- **CDC change detection** via SHA-256 row hashing of tracked attributes — enables efficient downstream change tracking without a native CDC feed

### 🥈 Silver — Cleansed & Conformed
- Deduplication, null-key filtering, VIN format validation
- **PII masking** — email hashed (SHA-256), phone numbers partially masked
- Standardization: casing, trimming, date typing
- Cross-domain enrichment via joins (sales ⨝ customer ⨝ dealer ⨝ vehicle)
- **SCD Type 2** dimension tracking for `customer`, `dealer`, and `vehicle_master` using DLT's native `apply_changes()` API — full historical versioning with `is_current` / `__START_AT` / `__END_AT` tracking
- Data quality enforced via DLT `@dlt.expect` constraints

### 🥇 Gold — Business KPIs
- `gold_sales_performance` — monthly units, revenue, discount %, channel mix
- `gold_production_efficiency` — completion rate, delay rate, throughput by plant/shift
- `gold_warranty_claims` — claim approval rates, claim value by part/model/dealer
- `gold_dealer_scorecard` — composite weighted performance score (revenue, feedback, warranty, inventory) with tiering (Platinum/Gold/Silver/Bronze)
- `gold_warranty_anomaly_behavioral` & `gold_discount_anomaly_behavioral` — z-score based behavioral anomaly detection
- `gold_daily_comparison` & `gold_daily_anomaly_summary` — day-over-day trend deltas and rolling 30-day anomaly flags, purpose-built to feed the LLM reporting layer

---

## 🤖 LLM-Powered Daily Business Summary

A scheduled Databricks job reads the Gold layer and generates a natural-language report using **Groq's Llama 3 70B**:

- **Trend comparison** — today vs. yesterday across sales, production, service, warranty, and inventory, with % change indicators
- **Anomaly detection** — flags statistically significant deviations (|z-score| > 2) against a rolling 30-day baseline, narrated in plain English with recommended actions
- **Dealer performance** — top/bottom 5 dealers by composite score
- **Delivery** — formatted with Slack Block Kit and posted directly to a dedicated channel
- **Audit logging** — every run logs token usage and delivery status to a Delta audit table

📷 *Sample Slack output:*

> **Daily Automobile Business Summary**
> Executive Summary, Sales, Production, Service, Warranty, Inventory breakdowns, ⚠️ Anomalies section, and Recommended Actions — generated fresh every day.

---

## ⚙️ Orchestration

Everything runs as a single **Databricks Job** with task-level dependencies:

```
ETL_pipeline (DLT)
   └── Data_Transformation_silver_layer
          ├── gold_kpis_for_dashboards ──▶ Analysis_dashboards_for_gold_kpis
          └── daily_summary_for_reporting ──▶ generate_report_using_LLM_send_update_to_slack
```

- **25 upstream tables, 15+ downstream tables** tracked via Databricks Unity Catalog lineage
- Fully serverless compute
- Performance-optimized job clusters

---

## 🛠️ Tech Stack

| Layer | Technology |
|---|---|
| Ingestion | Databricks Auto Loader (cloudFiles), S3 |
| Transformation | PySpark, Delta Live Tables (DLT) |
| Storage | Delta Lake |
| Orchestration | Databricks Jobs & Pipelines |
| Data Quality | DLT Expectations (`@dlt.expect`) |
| Change Tracking | SCD Type 2 (`apply_changes`), SHA-256 CDC hashing |
| LLM Reporting | Groq API (Llama 3 70B) |
| Delivery | Slack Block Kit (Incoming Webhooks) |
| Governance | Unity Catalog (lineage tracking) |

---

## 📂 Repository Structure

```
├── bronze.py                  # Auto Loader ingestion + watermarking + CDC hashing
├── silver.py                  # Cleansing, PII masking, SCD Type 2, enrichment
├── gold.py                    # KPI aggregation + behavioral anomaly detection
├── gold_daily_summary.py      # Daily trend comparison + rolling anomaly tables
├── llm_summariser.py          # Groq Llama summary generation + Slack delivery
└── README.md
```

---

## 🚀 Key Highlights

- ✅ Fully streaming medallion architecture on Delta Live Tables
- ✅ Production-grade **watermarking** for late-data handling
- ✅ **SCD Type 2** historization on all dimension tables
- ✅ **CDC-style change detection** without native database CDC feeds
- ✅ **Z-score based statistical anomaly detection** across 4 business domains
- ✅ **LLM-generated executive reporting**, delivered automatically to Slack
- ✅ End-to-end orchestration with full lineage visibility in Unity Catalog

---

## 📬 Connect

Built as a hands-on deep dive into modern Lakehouse architecture, streaming ETL, and LLM-powered analytics delivery.

🔗 LinkedIn: `https://www.linkedin.com/in/sai-manikanta-yashwanth-munagala-aab208352/`

---

*If you found this useful, feel free to ⭐ the repo or reach out — always happy to talk data engineering!*
