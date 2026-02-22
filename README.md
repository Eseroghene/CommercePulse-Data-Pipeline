
# CommercePulse Data Pipeline

> Building a reliable analytics platform from historical data and live events

## Overview

CommercePulse is an e-commerce aggregation platform operating across multiple African markets. This pipeline solves a critical data infrastructure challenge: combining **historical batch exports** from early operations with **live event streams** from the current platform into a unified analytics system.

The pipeline delivers trustworthy daily analytics on revenue, payment success rates, and refund patterns while handling real-world challenges like schema drift, duplicate events, and late arrivals.

---

## Table of Contents

- [Architecture](#architecture)
- [Quick Start](#quick-start)
- [Data Flow](#data-flow)
- [Key Analytics Queries](#key-analytics-queries)
- [Design Decisions](#design-decisions)
- [Data Quality](#data-quality)
- [Project Structure](#project-structure)
- [Results](#results)
- [Assumptions](#assumptions)
- [Known Limitations](#known-limitations)

---

## Architecture
```
Historical JSON Files  →  MongoDB (Raw Events)  →  Pandas (Transform)  →  BigQuery (Analytics)
Live Event Streams     →
```

**MongoDB** - Stores all raw events exactly as received (system of record)  
**Pandas** - Normalizes vendor schemas, handles duplicates and late arrivals  
**BigQuery** - Clean analytics tables for BI tools and reporting  

---

## Quick Start

### 1. Setup
```bash
# Install dependencies
pip install pymongo pandas python-dotenv google-cloud-bigquery faker

# Configure .env
MONGO_URI=mongodb://localhost:27017
MONGO_DB=commercepulse
BQ_PROJECT=your-gcp-project-id
BQ_DATASET=commercepulse
GOOGLE_APPLICATION_CREDENTIALS=keys/your-service-account.json
```

### 2. Run Pipeline
```bash
# Load historical data into MongoDB
python src/bootstrap_loader.py

# Generate and load live events
python src/live_event_generator.py --out data/live_events --events 2000
python src/live_event_loader.py 2026-02-19

# Transform and load to BigQuery
python src/bq_loader.py

# Generate quality report
python src/quality_report.py
```

---

## Data Flow

### MongoDB Collection: `events_raw`
- Stores historical + live events
- Deterministic `event_id` (SHA-256 hash)
- Idempotent upserts handle duplicates

### BigQuery Tables
- `fact_orders` - Current order state (upsert by order_id)
- `fact_payments` - Append-only payment log
- `fact_refunds` - Append-only refund log
- `fact_order_daily` - Daily revenue aggregates

---

## Key Analytics Queries

**Daily Revenue:**
```sql
SELECT order_date, SUM(gross_revenue), SUM(net_revenue)
FROM commercepulse.fact_order_daily
GROUP BY order_date;
```

**Payment Success Rate by Vendor:**
```sql
SELECT vendor, AVG(payment_success_rate) * 100 AS success_pct
FROM commercepulse.fact_order_daily
GROUP BY vendor;
```

---

## Design Decisions

### MongoDB vs BigQuery
- **MongoDB**: Schema-flexible raw storage, enables reprocessing, audit trail
- **BigQuery**: Fast SQL queries, optimized for analytics, BI tool integration

### Append-Only vs Upsert
- **Payments/Refunds**: Append-only preserves audit trail, all attempts recorded
- **Orders**: Upsert reflects current state, simplifies analytics queries

### Pandas Transformation
- Handles vendor schema drift expressively
- Trade-off: Limited to ~10M rows (memory-bound)
- Alternative: Migrate to Spark/dbt for scale

### Idempotency
- Deterministic event IDs prevent duplicates
- Safe to re-run pipeline without data duplication
- Enables recovery from failures

---

## Data Quality

The pipeline detects:
- Missing required fields (customer IDs, amounts)
- Orphan records (payments without orders)
- Late arrivals (payments >7 days after order)
- Revenue integrity (gross vs net, refund rates)

Reports saved to `reports/quality_report_YYYY-MM-DD.txt`

---

## Project Structure
```
commercepulse/
├── data/
│   ├── bootstrap/          
│   └── live_events/        
├── src/
│   ├── bootstrap_loader.py 
│   ├── live_event_generator.py
│   ├── live_event_loader.py 
│   ├── transformer.py      
│   ├── bq_loader.py        
│   └── quality_report.py   
├── reports/                
└── README.md
```

---

## Results

- **5,480 events** processed (3,480 historical + 2,000 live)
- **$5.1M net revenue** tracked
- **74.7% payment success rate**
- **251 orphan payments** detected (data quality issue flagged)

---

## Assumptions

- Payment status `success`/`failed` normalized from vendor variations
- Orders without matching payments excluded from revenue calculations
- `vendor = "unknown"` when vendor field missing from payload
- Historical data treated as synthetic events for unified processing

---

## Known Limitations

- Pandas transformation limited to ~50M rows (memory-bound)
- No real-time streaming (daily batch processing)
- Single BigQuery region (no multi-region replication)
- Vendor schema drift handled reactively (no schema registry)
