# E-Commerce Data Lakehouse Pipeline

An end-to-end data pipeline for a fake online store, built to practise putting
the usual data engineering tools together in one place: Kafka for events, Flink
for stream processing, Iceberg and Delta Lake for storage, Spark for batch jobs,
dbt for the warehouse models, Airflow to schedule it, and Great Expectations to
check the numbers at the end.

Everything runs locally with Docker Compose. MinIO stands in for S3 and Postgres
plays both the source database and the warehouse.

---

## Architecture

```
  clickstream_producer.py                generate_orders.py
           |                                     |
           v                                     v
     Kafka topic                          Postgres (public)
  clickstream.events                   users / orders / products
           |                                     |
           v                                     |
     Flink (PyFlink SQL)                         |
   - sessioniser.py                              |
   - windowed_aggregations.py                    |
           |                                     |
           v                                     |
     Iceberg tables on MinIO                     |
   lakehouse.raw.user_sessions                   |
   lakehouse.raw.product_metrics_5min            |
           |                                     |
           v                                     |
     Spark (daily batch)                         |
   - raw_to_curated.py      <-------------------/
   - user_features.py
           |
           +--> Delta Lake on MinIO  (history)
           |
           v
     Postgres (analytics schema)
           |
           v
     dbt  ->  staging -> marts
           |
           +--> FastAPI  (serving/main.py)
           +--> Metabase dashboards
           +--> Great Expectations checks
```

The streaming side produces browsing behaviour; the batch side has the order
history. They meet in dbt, where sessions and orders are joined into the funnel
and cohort models.

---

## Tech stack

| Tool | What it does here |
|------|-------------------|
| Kafka + Zookeeper | Carries the clickstream events |
| Flink 1.18 (PyFlink) | Session windows and 5-minute product counts |
| Apache Iceberg | Raw zone table format, written by Flink |
| Iceberg REST catalog | Catalog server so Flink and Spark see the same tables |
| Spark 3.5 | Daily batch job and RFM feature engineering |
| Delta Lake | Curated zone table format, keeps daily history |
| MinIO | S3-compatible object storage for local development |
| Postgres 16 | Source database, `analytics` schema, dbt warehouse, Airflow metadata |
| dbt (Postgres adapter) | Staging and mart models, plus tests |
| Airflow 2.8 | Runs the daily pipeline |
| Great Expectations | Data quality checks on the final tables |
| FastAPI | Small read API over the analytics tables |
| Metabase | Dashboards |

---

## Repository layout

```
LakeHousePipeline/
├── ingestion/
│   ├── producers/
│   │   ├── config.py
│   │   └── clickstream_producer.py     # JSON events -> Kafka
│   ├── postgres/init.sql               # source schema + product catalogue
│   └── generate_orders.py              # fills Postgres with users and orders
├── streaming/flink_jobs/
│   ├── config.py
│   ├── iceberg_utils.py                # Kafka source + Iceberg catalog setup
│   ├── sessioniser.py                  # 30-minute session windows
│   └── windowed_aggregations.py        # 5-minute product metrics
├── batch/spark_jobs/
│   ├── config.py
│   ├── spark_session.py
│   ├── raw_to_curated.py               # Iceberg -> Delta -> Postgres
│   └── user_features.py                # RFM scores and LTV estimate
├── dbt/
│   ├── models/staging/                 # stg_orders, stg_users, stg_sessions, ...
│   ├── models/marts/                   # fct_orders, dim_users, mart_*
│   ├── macros/ and tests/
│   └── dbt_project.yml, profiles.yml
├── orchestration/dags/daily_pipeline.py
├── quality/run_data_checks.py
├── serving/main.py                     # FastAPI
├── notebooks/exploratory_analysis.ipynb
├── docker-compose.yml
└── requirements.txt
```

---

## Getting started

### What you need

- Docker Desktop with about 8 GB of RAM available
- Python 3.11

### 1. Start the stack

```bash
cp .env.example .env
docker compose up -d
```

Give it a couple of minutes. The Airflow containers install extra Python
packages on first boot, so they take the longest.

| Service | URL | Login |
|---------|-----|-------|
| Kafka UI | http://localhost:8080 | — |
| MinIO console | http://localhost:9001 | minioadmin / minioadmin |
| Flink dashboard | http://localhost:8082 | — |
| Airflow | http://localhost:8888 | admin / admin |
| Metabase | http://localhost:3000 | set up on first visit |

### 2. Install the Python packages

```bash
pip install -r requirements.txt
```

### 3. Load the order history

```bash
python ingestion/generate_orders.py --users 2000 --orders 20000 --days 180
```

### 4. Start the clickstream producer

```bash
python ingestion/producers/clickstream_producer.py
```

Leave it running in its own terminal. Events show up in Kafka UI under the
`clickstream.events` topic.

### 5. Submit the Flink jobs

```bash
docker exec flink-jobmanager flink run -py /opt/flink/jobs/flink_jobs/sessioniser.py
docker exec flink-jobmanager flink run -py /opt/flink/jobs/flink_jobs/windowed_aggregations.py
```

Both jobs keep running and write to Iceberg. You can watch them on the Flink
dashboard. Sessions only close after 30 minutes of inactivity for a user, so it
takes a while before the first rows appear — the product metrics job is quicker
because it emits every 5 minutes.

### 6. Run the batch jobs

Either trigger the `daily_pipeline` DAG from the Airflow UI, or run the steps
yourself:

```bash
python batch/spark_jobs/raw_to_curated.py 2026-09-17
python batch/spark_jobs/user_features.py 2026-09-17

cd dbt
dbt deps
dbt build --profiles-dir .

cd ..
python quality/run_data_checks.py
```

### 7. Look at the results

```bash
uvicorn serving.main:app --reload     # http://localhost:8000/docs
jupyter notebook notebooks/exploratory_analysis.ipynb
```

Or point Metabase at the Postgres database (host `postgres`, database
`ecommerce`, user `postgres`) and build dashboards on the `marts` schema.

---

## Clickstream event format

The producer sends plain JSON:

```json
{
  "event_id": "9d3e...",
  "event_type": "add_to_cart",
  "user_id": "USR-0421",
  "session_id": "1a2b...",
  "product_id": "PROD-017",
  "device_type": "mobile",
  "country": "US",
  "event_time": "2026-09-17 14:32:08.412"
}
```

`event_type` is one of `page_view`, `product_view`, `search`, `add_to_cart`,
`checkout_start` or `purchase`. The Flink jobs declare this shape as a Kafka
table with `'format' = 'json'` and a watermark on `event_time`.

---

## dbt models

| Model | Layer | Grain |
|-------|-------|-------|
| `stg_orders` | staging | one row per order |
| `stg_order_items` | staging | one row per line item |
| `stg_users` | staging | one row per user |
| `stg_products` | staging | one row per product |
| `stg_sessions` | staging | one row per browsing session |
| `fct_orders` | marts | one row per order, items rolled up |
| `dim_users` | marts | one row per user, with RFM features |
| `dim_products` | marts | one row per product, with 30-day engagement |
| `mart_conversion_funnel` | marts | one row per day |
| `mart_cohort_retention` | marts | one row per cohort month and offset |

Useful commands:

```bash
cd dbt
dbt build --profiles-dir .                 # run + test everything
dbt run --profiles-dir . --select marts    # just the marts
dbt docs generate --profiles-dir . && dbt docs serve --profiles-dir .
```

---

## RFM scoring

`user_features.py` scores every user on three things over the last 180 days:

- **Recency** — days since their last order
- **Frequency** — how many orders they placed
- **Monetary** — how much they spent

Each one is split into five equal buckets with `ntile(5)`, so a score of 5 is
always the best fifth of users. Recency is scored in reverse because fewer days
is better. The segment then comes from recency plus the average of the other
two — for example, a user scoring high on all three is a "champion", and one who
used to buy a lot but has not been back is "at risk".

There is also a rough LTV estimate: what they spent in the window, scaled up to
a year and nudged upwards for frequent buyers. It is a heuristic, not a model.

---

## Things I would do differently

- The Spark jobs copy their output into Postgres with a full table overwrite.
  That is fine at this data size but would need to become incremental for
  anything real.
- Sessions are keyed by user id, so a user browsing on two devices at once ends
  up in one session. The session id from the producer is ignored in favour of
  the Flink window.
- There is no schema registry. JSON keeps the project simple, but a real setup
  would want Avro or Protobuf so producers cannot break consumers.
- The LTV estimate is a formula, not a trained model.
- Airflow installs its extra dependencies at container start, which is slow. A
  custom image would be the proper fix.
