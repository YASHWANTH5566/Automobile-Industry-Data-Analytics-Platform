# 🚗 Automobile Industry Analytics Platform

### Databricks Lakehouse | Delta Live Tables (DLT) | Medallion Architecture

An end-to-end **Automobile Industry Analytics Platform** built on the **Databricks Lakehouse Architecture** using **Delta Live Tables (DLT)** and **PySpark Structured Streaming**.

This project ingests raw automobile industry datasets from Amazon S3, processes them through a **Bronze → Silver → Gold** medallion architecture, and delivers business-ready KPI tables powering executive dashboards for:

* Sales Analytics
* Production Efficiency
* Warranty & Service Insights
* Dealer Performance Scorecards
* Behavioral Anomaly Detection

---

# 📌 Architecture Overview

```text
                ┌──────────────────────┐
                │      Raw CSV Data     │
                │      (Amazon S3)      │
                └──────────┬───────────┘
                           │
                           ▼
                 ┌──────────────────┐
                 │  Bronze Layer     │
                 │ Auto Loader (DLT) │
                 └────────┬─────────┘
                          │
                          ▼
                 ┌──────────────────┐
                 │  Silver Layer     │
                 │ Cleansing & QA    │
                 └────────┬─────────┘
                          │
                          ▼
                 ┌──────────────────┐
                 │   Gold Layer      │
                 │ KPI Aggregations  │
                 └────────┬─────────┘
                          │
                          ▼
               ┌────────────────────┐
               │ BI Dashboards       │
               │ Power BI / DBSQL    │
               └────────────────────┘
```

---

# 🏗️ Tech Stack

| Technology              | Purpose                     |
| ----------------------- | --------------------------- |
| Databricks              | Unified Analytics Platform  |
| Delta Live Tables (DLT) | Declarative ETL Pipelines   |
| PySpark                 | Distributed Data Processing |
| Structured Streaming    | Real-time ingestion         |
| Amazon S3               | Data Lake Storage           |
| Delta Lake              | ACID Lakehouse Storage      |
| Power BI / Tableau      | Dashboarding & BI           |
| Unity Catalog           | Governance & Security       |

---

# 📂 Project Structure

```text
automobile-analytics-platform/
│
├── bronze_layer.py
├── silver_layer.py
├── gold_layer.py
├── dashboards/
│   ├── sales_dashboard.md
│   ├── production_dashboard.md
│   ├── warranty_dashboard.md
│   └── dealer_scorecard_dashboard.md
│
├── architecture/
│   └── medallion_architecture.png
│
├── README.md
└── requirements.txt
```

---

# 🥉 Bronze Layer – Raw Data Ingestion

The Bronze Layer ingests raw CSV files from S3 using:

* Databricks Auto Loader (`cloudFiles`)
* Structured Streaming
* Schema Inference
* Incremental Processing
* Checkpointing

## Source Tables

| Table                     | Description                |
| ------------------------- | -------------------------- |
| bronze_customer_new       | Customer master            |
| bronze_sales_new          | Vehicle sales transactions |
| bronze_dealer_new         | Dealer information         |
| bronze_inventory_new      | Inventory stock            |
| bronze_parts_new          | Spare parts master         |
| bronze_production_new     | Production records         |
| bronze_service_new        | Service transactions       |
| bronze_vehicle_master_new | Vehicle specifications     |
| bronze_warranty_new       | Warranty claims            |
| bronze_dealer_parts_new   | Dealer parts inventory     |

---

# 🥈 Silver Layer – Cleansing & Enrichment

The Silver Layer performs:

## ✅ Data Quality Operations

* Deduplication
* Null validation
* VIN validation
* Type casting
* Standardization
* Data enrichment
* Derived KPI columns

## 🔒 PII Masking

Sensitive customer data is protected using:

* SHA256 hashing for emails
* Contact masking
* Controlled PII exposure

## 🔄 Business Transformations

Examples:

* Net Revenue Calculation
* Warranty Flags
* Inventory Health Indicators
* Production Completion Flags
* Dealer Enrichment
* Vehicle Enrichment

---

# 🥇 Gold Layer – Business KPI Tables

The Gold Layer contains business-ready analytical tables optimized for BI dashboards.

---

## 1️⃣ Gold Sales Performance

### Table:

`gold_sales_performance`

### KPIs

* Units Sold
* Gross Revenue
* Net Revenue
* Discount %
* Revenue Per Unit
* Active Dealers
* Unique Customers

### Business Use Cases

* Revenue trend analysis
* Regional sales monitoring
* Fuel type demand analysis
* Channel performance

---

## 2️⃣ Gold Production Efficiency

### Table:

`gold_production_efficiency`

### KPIs

* Total Units Produced
* Completion Rate %
* Delay Rate %
* Average Production Time
* Shift Efficiency
* Plant Throughput

### Business Use Cases

* Manufacturing optimization
* Bottleneck detection
* Shift-level monitoring
* Plant benchmarking

---

## 3️⃣ Gold Warranty Claims

### Table:

`gold_warranty_claims`

### KPIs

* Total Claims
* Approval Rate %
* Rejection Rate %
* Claim Amount
* Part Failure Trends
* Service Satisfaction

### Business Use Cases

* Warranty cost optimization
* Quality issue detection
* Dealer warranty analysis
* Parts reliability tracking

---

## 4️⃣ Gold Dealer Scorecard

### Table:

`gold_dealer_scorecard`

### KPIs

* Composite Dealer Score
* Revenue Performance
* Service Feedback
* Inventory Health
* Warranty Approval Rate
* Dealer Tier Classification

### Dealer Tiers

| Tier     | Score |
| -------- | ----- |
| PLATINUM | ≥ 80  |
| GOLD     | ≥ 60  |
| SILVER   | ≥ 40  |
| BRONZE   | < 40  |

### Business Use Cases

* Dealer ranking
* Incentive programs
* Regional dealer benchmarking
* Supply chain optimization

---

## 5️⃣ Behavioral Anomaly Detection

### Tables

* `gold_warranty_anomaly_behavioral`
* `gold_discount_anomaly_behavioral`

### Detection Logic

Statistical anomaly detection using:

* Historical dealer behavior
* Z-score analysis
* Claim rate deviation
* Discount leakage detection

### Example

```python
z_score = (current_rate - avg_rate) / stddev
```

Anomalies are flagged when:

```text
z_score > 2
```

---

# 📊 Dashboard Suite

---

# 📈 Dashboard 1 – Sales Performance

### Visualizations

* KPI Summary Cards
* Monthly Revenue Trend
* Units Sold by Model
* Revenue by Region
* Channel & Payment Mix

### Primary Users

* Sales Teams
* Finance
* Marketing Leadership

---

# 🏭 Dashboard 2 – Production Efficiency

### Visualizations

* Plant Throughput Trends
* Completion vs Delay Rate
* Shift Efficiency Heatmap
* Production Time Analysis

### Primary Users

* Manufacturing
* Operations
* Plant Managers

---

# 🛠️ Dashboard 3 – Warranty Claims Analytics

### Visualizations

* Claims Trend
* Claims by Part Category
* Dealer Warranty Performance
* Approval Rate Gauges

### Primary Users

* Quality Teams
* After-Sales
* Warranty Operations

---

# 🏆 Dashboard 4 – Dealer Scorecard

### Visualizations

* Dealer Ranking Leaderboard
* Composite Score Radar Chart
* Revenue vs Feedback Scatter
* Inventory Health Heatmap

### Primary Users

* Dealer Management
* Regional Managers
* Executive Leadership

---

# ⚡ Key Features

## 🚀 Real-Time Streaming Ingestion

Incremental ingestion using Databricks Auto Loader.

## 🔒 Data Governance & Security

PII masking and controlled access using Unity Catalog.

## 📊 Business-Ready KPIs

Curated Gold tables optimized for BI tools.

## 🧠 Behavioral Analytics

Statistical anomaly detection using z-score models.

## 📈 Scalable Lakehouse Architecture

Built on Delta Lake with ACID guarantees.

---

# 🧪 Data Quality Expectations

Implemented using DLT Expectations:

```python
@dlt.expect("valid_sale_amount", "sale_amount > 0")
@dlt.expect("vin_not_null", "vin IS NOT NULL")
```

Examples:

* Positive sales amount
* Non-null VINs
* Valid production time
* Valid warranty claim amounts

---

# 🔧 Optimization Strategies

* Delta OPTIMIZE + ZORDER
* Auto Optimize
* Partition pruning
* Materialized summaries
* Cached aggregations

---

# 🔐 Security & Governance

* Unity Catalog Integration
* Row-Level Security
* PII Hashing & Masking
* Audit Columns (`_processed_ts`)

---

# 📅 Refresh Schedule

| Dashboard             | Frequency |
| --------------------- | --------- |
| Sales Performance     | Daily     |
| Production Efficiency | Daily     |
| Warranty Claims       | Weekly    |
| Dealer Scorecard      | Monthly   |

---

# 📡 Alerting Rules

| Condition                     | Action                  |
| ----------------------------- | ----------------------- |
| Delay Rate > 25%              | Slack Alert             |
| Approval Rate < 50%           | Email Notification      |
| Dealer Score Drop > 15%       | Dealer Manager Alert    |
| Inventory Below Reorder > 60% | Supply Chain Escalation |

---

# 🚀 Future Enhancements

* ML-based demand forecasting
* Predictive maintenance
* Real-time IoT integration
* Dealer churn prediction
* Advanced anomaly detection with MLflow
* Streaming dashboards

---

# 📷 Suggested Dashboard Screenshots

Add screenshots here:

```text
/docs/screenshots/
```

Recommended:

* Sales Dashboard
* Production Dashboard
* Dealer Scorecard
* Warranty Analytics

---

# ▶️ How to Run

## 1. Configure S3 Paths

Update:

```python
.load("s3://automobile-pipeline/Raw/sales_new/")
```

with your own bucket paths.

---

## 2. Create DLT Pipeline

In Databricks:

```text
Workflows → Delta Live Tables → Create Pipeline
```

Attach:

* Bronze notebook
* Silver notebook
* Gold notebook

---

## 3. Run Pipeline

Choose:

```text
Triggered or Continuous Mode
```

---

## 4. Connect BI Tool

Use:

* Databricks SQL Endpoint
* Unity Catalog Gold Tables

Example:

```text
Catalog : automobile
Schema  : gold
```

---

# 📚 Learning Outcomes

This project demonstrates:

* Databricks Lakehouse Engineering
* Streaming ETL Pipelines
* Medallion Architecture
* Data Modeling
* Delta Live Tables
* Data Quality Engineering
* KPI Engineering
* BI Dashboard Design
* Data Governance
* Scalable Analytics Architecture

---

# 👨‍💻 Author

### M. S. M. Yashwanth

Data Engineer | Analytics Engineer | Lakehouse Enthusiast

---

# 📜 License

This project is licensed under the MIT License.

---
