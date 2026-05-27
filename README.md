# Real-Time Analytics & Data Lakehouse Pipeline

A production-grade data platform that ingests e-commerce clickstream and transactional events, processes them through a streaming layer (Apache Flink + Apache Iceberg), a batch curation layer (Apache Spark + Delta Lake), and a semantic warehouse layer (dbt on Snowflake), with Airflow orchestration, Great Expectations data quality, and Metabase dashboards. The entire local stack runs in Docker Compose; production infrastructure is provisioned by Terraform on AWS.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│  INGESTION LAYER                                                         │
│  Postgres CDC  ──┐                                                       │
│  Clickstream     ├──► Kafka (MSK / local)  ◄──  Schema Registry (Avro)  │
│  Search events   │                                                       │
│  Order events  ──┘                                                       │
└───────────────────────────────┬─────────────────────────────────────────┘
                                │
┌───────────────────────────────▼─────────────────────────────────────────┐
│  STREAM PROCESSING LAYER (Apache Flink 1.18)                             │
│  sessioniser.py          – session boundary detection (30-min gap)       │
│  windowed_aggregations.py – 5-min tumbling windows per product/user      │
│  iceberg_utils.py         – write to Apache Iceberg via REST catalog     │
│  Sink: S3 raw zone  →  lakehouse-raw/warehouse/  (Iceberg tables)       │
└───────────────────────────────┬─────────────────────────────────────────┘
                                │
┌───────────────────────────────▼─────────────────────────────────────────┐
│  BATCH PROCESSING LAYER (Apache Spark on EMR / local)                    │
│  raw_to_curated.py     – Iceberg → Delta Lake ETL (daily)                │
│  user_features.py      – RFM scoring + LTV estimation (Ray distributed) │
│  product_embeddings.py – item2vec embeddings for recommendations         │
│  Sink: S3 curated zone → lakehouse-curated/  (Delta Lake tables)         │
└───────────────────────────────┬─────────────────────────────────────────┘
                                │
┌───────────────────────────────▼─────────────────────────────────────────┐
│  SERVING & SEMANTIC LAYER                                                │
│  dbt on Snowflake  – staging/ → marts/ (fct_orders, dim_users,          │
│                       mart_conversion_funnel, mart_cohort_retention)     │
│  FastAPI           – /features/{user_id}, /recommendations/{product_id}  │
│  Metabase          – executive dashboards connecting to Snowflake MARTS  │
│  Great Expectations – validation suites on all three layers              │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## Technology Stack

| Technology              | Role                                                      | Layer           |
|-------------------------|-----------------------------------------------------------|-----------------|
| Apache Kafka (MSK)      | Event streaming backbone                                  | Ingestion       |
| Confluent Schema Registry | Avro schema enforcement on all topics                  | Ingestion       |
| Apache Flink 1.18       | Stateful stream processing, session detection             | Stream          |
| Apache Iceberg          | Raw zone table format; ACID on streaming writes           | Stream          |
| Iceberg REST Catalog    | Catalog server (tabulario/iceberg-rest)                   | Stream          |
| Apache Spark (EMR 6.15) | Batch ETL, feature engineering                            | Batch           |
| Delta Lake              | Curated zone table format; time-travel, Z-ordering        | Batch           |
| Ray                     | Distributed feature engineering across Spark workers      | Batch           |
| dbt (Snowflake adapter) | SQL transformations, mart layer, documentation            | Serving         |
| Snowflake               | Cloud data warehouse; semantic layer for BI               | Serving         |
| FastAPI                 | REST API serving user features and recommendations        | Serving         |
| Metabase v0.49          | Self-service BI dashboards                                | Serving         |
| Great Expectations 0.18 | Data quality validation at all three layers               | Quality         |
| Apache Airflow 2.8      | DAG orchestration (batch jobs, dbt, GE checkpoints)       | Orchestration   |
| MinIO                   | S3-compatible local object storage                        | Infrastructure  |
| Terraform (AWS provider)| IaC for MSK, EMR, S3, Snowflake resources                 | Infrastructure  |
| PostgreSQL 16           | Source-of-truth OLTP database; Airflow backend            | Infrastructure  |

---

## Repository Structure

```
LakeHousePipeline/
├── infra/
│   └── terraform/
│       ├── main.tf          # Provider config + S3 backend
│       ├── variables.tf     # All input variables
│       ├── s3.tf            # Raw, curated, logs, state buckets
│       ├── kafka.tf         # MSK cluster + security group
│       ├── emr.tf           # Spark cluster + IAM roles
│       ├── snowflake.tf     # Snowflake DB, schemas, warehouse, stage
│       └── outputs.tf       # Exported resource identifiers
├── streaming/
│   ├── flink_jobs/
│   │   ├── config.py
│   │   ├── iceberg_utils.py
│   │   ├── sessioniser.py
│   │   └── windowed_aggregations.py
│   └── schemas/             # Avro schemas for all Kafka topics
│       ├── clickstream.avsc
│       ├── cart_event.avsc
│       ├── order_event.avsc
│       ├── search_event.avsc
│       ├── user_event.avsc
│       └── inventory_event.avsc
├── batch/
│   └── spark_jobs/
│       ├── config.py
│       ├── spark_session.py
│       ├── raw_to_curated.py
│       ├── user_features.py
│       └── product_embeddings.py
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── packages.yml
│   ├── models/
│   │   ├── staging/         # stg_orders, stg_users, stg_events
│   │   └── marts/           # fct_orders, dim_users, mart_*
│   ├── macros/
│   ├── seeds/
│   └── tests/
├── orchestration/
│   └── dags/                # Airflow DAG files
├── ingestion/
│   ├── producers/           # Python Kafka producers
│   │   ├── clickstream_producer.py
│   │   ├── order_producer.py
│   │   ├── cart_producer.py
│   │   ├── search_producer.py
│   │   ├── user_producer.py
│   │   ├── inventory_producer.py
│   │   └── run_all.py
│   └── postgres/
│       └── init.sql         # Schema + seed data
├── quality/
│   ├── great_expectations/
│   │   └── great_expectations.yml
│   ├── expectations/
│   │   ├── raw_suite.py
│   │   ├── curated_suite.py
│   │   └── warehouse_suite.py
│   ├── checkpoints/
│   │   ├── raw_checkpoint.yml
│   │   ├── curated_checkpoint.yml
│   │   └── warehouse_checkpoint.yml
│   └── ab_testing.py
├── notebooks/
│   └── exploratory_analysis.ipynb
├── docker-compose.yml
├── requirements.txt
├── .env.example
└── README.md
```

---

## Quick Start

### Prerequisites

- Docker Desktop >= 24 with at least 8 GB RAM allocated
- Python 3.11+
- Terraform >= 1.6 (for cloud deployment only)
- A Snowflake trial or paid account (for dbt/warehouse features)

### Steps

1. **Clone the repository**
   ```bash
   git clone https://github.com/your-org/lakehouse-pipeline.git
   cd lakehouse-pipeline
   ```

2. **Configure environment variables**
   ```bash
   cp .env.example .env
   # Edit .env — at minimum set SNOWFLAKE_* variables
   ```

3. **Start the local stack**
   ```bash
   docker compose up -d
   # Wait ~60 s for all health checks to pass
   docker compose ps
   ```

4. **Create Kafka topics**
   ```bash
   docker exec kafka kafka-topics \
     --bootstrap-server localhost:9092 \
     --create --topic clickstream.events \
     --partitions 6 --replication-factor 1

   docker exec kafka kafka-topics \
     --bootstrap-server localhost:9092 \
     --create --topic ecommerce.orders \
     --partitions 6 --replication-factor 1

   docker exec kafka kafka-topics \
     --bootstrap-server localhost:9092 \
     --create --topic ecommerce.cart_events \
     --partitions 6 --replication-factor 1

   docker exec kafka kafka-topics \
     --bootstrap-server localhost:9092 \
     --create --topic ecommerce.search_events \
     --partitions 6 --replication-factor 1

   docker exec kafka kafka-topics \
     --bootstrap-server localhost:9092 \
     --create --topic ecommerce.user_events \
     --partitions 6 --replication-factor 1

   docker exec kafka kafka-topics \
     --bootstrap-server localhost:9092 \
     --create --topic ecommerce.inventory_events \
     --partitions 3 --replication-factor 1
   ```

5. **Run the Kafka producers**
   ```bash
   pip install -r requirements.txt
   python ingestion/producers/run_all.py
   ```

6. **Submit Flink jobs**
   ```bash
   # Session detection
   docker exec flink-jobmanager flink run \
     -py /opt/flink/jobs/flink_jobs/sessioniser.py

   # Windowed aggregations (writes to Iceberg)
   docker exec flink-jobmanager flink run \
     -py /opt/flink/jobs/flink_jobs/windowed_aggregations.py
   ```

7. **Trigger the Airflow batch DAG**

   Open http://localhost:8888 (admin / admin), enable the `lakehouse_daily_pipeline` DAG, and trigger a manual run. The DAG runs: Spark raw_to_curated → user_features → dbt build → GE checkpoints.

---

## Kafka Topics

| Topic                       | Partitions | Schema File              | Producer                    | Consumer          |
|-----------------------------|------------|---------------------------|-----------------------------|-------------------|
| `clickstream.events`        | 6          | clickstream.avsc          | clickstream_producer.py     | Flink sessioniser |
| `ecommerce.orders`          | 6          | order_event.avsc          | order_producer.py           | Flink + Spark     |
| `ecommerce.cart_events`     | 6          | cart_event.avsc           | cart_producer.py            | Flink aggregations|
| `ecommerce.search_events`   | 6          | search_event.avsc         | search_producer.py          | Flink aggregations|
| `ecommerce.user_events`     | 6          | user_event.avsc           | user_producer.py            | Spark user_features|
| `ecommerce.inventory_events`| 3          | inventory_event.avsc      | inventory_producer.py       | Spark raw_to_curated|

All topics use Avro serialization validated against the Confluent Schema Registry at `http://localhost:8081`.

---

## Data Flow

A single purchase event travels through the pipeline as follows:

1. **Browser** fires a `purchase` clickstream event (JSON) to the web backend.
2. **Backend** serializes it to Avro (validated against `clickstream.avsc`) and publishes it to `clickstream.events` partition determined by `user_id`.
3. **Flink sessioniser** consumes the event, detects session boundaries (30-minute inactivity gap), and emits enriched session records.
4. **Flink windowed_aggregations** computes 5-minute tumbling window aggregates (views, add-to-cart counts, purchase counts per product) and writes them to the **Iceberg** table `lakehouse.raw.windowed_product_aggregations` via the REST catalog. Data lands in `s3://lakehouse-raw/warehouse/`.
5. **Airflow** triggers the daily **Spark** `raw_to_curated` job at 02:00 UTC. Spark reads all Iceberg raw tables, applies deduplication and type normalization, and writes partitioned **Delta Lake** tables to `s3://lakehouse-curated/`.
6. **Spark** `user_features` job reads Delta Lake curated orders and computes RFM scores and LTV estimates via a **Ray** remote function. Results are written back to `s3://lakehouse-curated/user_features/`.
7. **Airflow** runs `dbt build` against Snowflake. dbt reads from Snowflake external tables (pointing to the curated S3 zone via the `S3_LAKEHOUSE_INT` storage integration) and produces mart tables including `fct_orders`, `dim_users`, and `mart_conversion_funnel`.
8. **Airflow** runs the three **Great Expectations** checkpoints. Failures post a Slack notification and block downstream DAG tasks.
9. **Metabase** queries `LAKEHOUSE.MARTS.*` live from Snowflake. **FastAPI** serves `/features/{user_id}` from a Redis-backed cache populated by the user_features job.

---

## dbt Project

### Run locally

```bash
cd dbt
pip install dbt-snowflake
dbt deps          # install packages from packages.yml
dbt seed          # load CSV seeds
dbt build         # run + test all models
dbt docs generate && dbt docs serve   # opens lineage graph at localhost:8080
```

### Available models

| Model                     | Schema   | Type        | Grain                       |
|---------------------------|----------|-------------|-----------------------------|
| `stg_orders`              | staging  | view        | one row per order           |
| `stg_users`               | staging  | view        | one row per user            |
| `stg_events`              | staging  | incremental | one row per clickstream event|
| `fct_orders`              | marts    | incremental | one row per order           |
| `dim_users`               | marts    | table       | one row per user            |
| `mart_conversion_funnel`  | marts    | incremental | one row per day             |
| `mart_cohort_retention`   | marts    | table       | one row per cohort-week     |

### View lineage

```bash
dbt docs generate && dbt docs serve
```

Lineage graph is available at `http://localhost:8080`. The full DAG shows `raw_sources → stg_* → fct_orders / dim_users → mart_*`.

---

## Great Expectations

### Run validation suites

```bash
cd quality

# Build suites (only needed once or after changing expectations)
python expectations/raw_suite.py
python expectations/curated_suite.py
python expectations/warehouse_suite.py

# Run checkpoints against live data
great_expectations checkpoint run raw_checkpoint
great_expectations checkpoint run curated_checkpoint
great_expectations checkpoint run warehouse_checkpoint
```

### View reports

Data docs are generated at `quality/great_expectations/uncommitted/data_docs/local_site/index.html`. Open in a browser after running any checkpoint.

### Checkpoint behaviour

All three checkpoints are configured to:
- Store validation results locally (and optionally in S3 for production)
- Update Data Docs on every run
- Send a Slack notification to `SLACK_WEBHOOK_URL` **only on failure**

---

## A/B Testing Module

`quality/ab_testing.py` performs post-hoc analysis of experiments stored in `MARTS.ab_test_assignments`.

```bash
# Set Snowflake env vars, then:
python quality/ab_testing.py --test-name "checkout_redesign_2024q4"
```

The script runs three analyses in sequence:

1. **Mann-Whitney U test** on `purchase_count` and `total_spend` (non-parametric, appropriate for heavy-tailed revenue distributions). Reports p-value, Cohen's d effect size, and relative lift %.

2. **Causal inference via DoWhy** — estimates the Average Treatment Effect (ATE) using backdoor adjustment to control for `acquisition_channel` confounding. Runs two refutation tests (random common cause, placebo treatment) to validate the causal estimate.

3. **Kaplan-Meier survival analysis** on time-to-second-purchase. Fits separate KM curves for control and treatment, then computes a log-rank test p-value to determine whether the treatment accelerates repeat purchase.

Results are printed to stdout in a structured summary and returned as a dictionary for programmatic use.

---

## Infrastructure

### Deploy to AWS

```bash
cd infra/terraform

# First-time: create the Terraform state bucket manually
aws s3 mb s3://lakehouse-terraform-state --region us-east-1

terraform init
terraform plan -var-file=prod.tfvars
terraform apply -var-file=prod.tfvars
```

### What gets created

| Resource               | Service         | Notes                                             |
|------------------------|-----------------|---------------------------------------------------|
| 3 × S3 buckets         | Amazon S3       | raw (730d expiry), curated (no expiry), logs (365d)|
| 1 × S3 state bucket    | Amazon S3       | Terraform remote state                            |
| MSK cluster            | Amazon MSK      | 3-broker Kafka 3.5.1, TLS_PLAINTEXT, 1 TB storage |
| MSK configuration      | Amazon MSK      | auto-create=false, 6 default partitions, ISR=2    |
| Schema Registry URL    | AWS SSM         | SecureString; update with Confluent SR endpoint   |
| EMR cluster            | Amazon EMR 6.15 | Spark + Hive + Livy; auto-terminates after 1h idle|
| IAM roles (2)          | AWS IAM         | emr-service-role, emr-ec2-role                    |
| Snowflake database     | Snowflake       | LAKEHOUSE with RAW, STAGING, MARTS, EXTERNAL schemas|
| Snowflake warehouse    | Snowflake       | COMPUTE_WH, X-SMALL, auto-suspend 5 min           |
| Snowflake stage        | Snowflake       | S3_RAW_STAGE pointing to raw bucket               |
| Storage integration    | Snowflake       | S3_LAKEHOUSE_INT covering raw + curated buckets   |
| External tables (2)    | Snowflake       | USER_FEATURES, PRODUCT_PERFORMANCE in EXTERNAL    |

---

## Development Guide

### Adding a new Kafka topic

1. Create an Avro schema in `streaming/schemas/<topic_name>.avsc`.
2. Register the schema against the Schema Registry:
   ```bash
   curl -X POST http://localhost:8081/subjects/<topic_name>-value/versions \
     -H "Content-Type: application/vnd.schemaregistry.v1+json" \
     -d "{\"schema\": $(cat streaming/schemas/<topic_name>.avsc | jq -Rs .)}"
   ```
3. Create the Kafka topic (see Quick Start step 4 for the command template).
4. Add a producer in `ingestion/producers/` following the pattern in `clickstream_producer.py`.
5. Add a Flink consumer in `streaming/flink_jobs/` if real-time aggregation is needed.
6. Add the topic to the Kafka Topics table in this README.

### Adding a new dbt model

1. Create the SQL file under `dbt/models/staging/` or `dbt/models/marts/`.
2. Define column-level tests and descriptions in the matching `.yml` schema file (or create one).
3. If the model is a new mart table exposed to Metabase, grant SELECT to the REPORTER role:
   ```sql
   GRANT SELECT ON TABLE LAKEHOUSE.MARTS.<model_name> TO ROLE REPORTER;
   ```
4. Add a Great Expectations expectation to `quality/expectations/warehouse_suite.py` and re-run the suite builder.
5. Run `dbt build --select <model_name>` to validate before merging.

### Coding conventions

- **Python**: Black formatting, 100-character line limit, type hints on all function signatures, no bare `except`.
- **SQL (dbt)**: CTEs over subqueries. Use `{{ ref() }}` for all inter-model references. Primary key tests on every mart model.
- **Terraform**: One resource type per file. All resources tagged with `Environment`, `Project`, and `Name`.
- **Avro schemas**: `snake_case` field names. All nullable fields must include `"null"` in the union type.

---

## Environment Variables

| Variable                  | Required | Default        | Description                                             |
|---------------------------|----------|----------------|---------------------------------------------------------|
| `SNOWFLAKE_ACCOUNT`       | Yes      | —              | Account identifier in `orgname-accountname` format      |
| `SNOWFLAKE_USER`          | Yes      | —              | Service account username                                |
| `SNOWFLAKE_PASSWORD`      | Yes      | —              | Service account password                                |
| `SNOWFLAKE_DATABASE`      | No       | `LAKEHOUSE`    | Snowflake database name                                 |
| `SNOWFLAKE_WAREHOUSE`     | No       | `COMPUTE_WH`   | Snowflake virtual warehouse                             |
| `SNOWFLAKE_ROLE`          | No       | `SYSADMIN`     | Snowflake role for the session                          |
| `KAFKA_BOOTSTRAP_SERVERS` | Yes      | `localhost:9092`| Kafka broker list                                      |
| `SCHEMA_REGISTRY_URL`     | Yes      | `http://localhost:8081` | Confluent Schema Registry endpoint            |
| `MINIO_ROOT_USER`         | No       | `minioadmin`   | MinIO access key (local development only)               |
| `MINIO_ROOT_PASSWORD`     | No       | `minioadmin`   | MinIO secret key (local development only)               |
| `MINIO_ENDPOINT`          | No       | `http://localhost:9000` | MinIO endpoint for PyArrow / Spark S3A       |
| `AWS_ACCESS_KEY_ID`       | Prod     | —              | AWS access key (not needed locally with MinIO)          |
| `AWS_SECRET_ACCESS_KEY`   | Prod     | —              | AWS secret key                                          |
| `AWS_REGION`              | Prod     | `us-east-1`    | AWS region for S3, MSK, EMR                             |
| `SLACK_WEBHOOK_URL`       | No       | —              | Incoming webhook URL for GE failure notifications       |
| `AIRFLOW__WEBSERVER__SECRET_KEY` | Yes | —          | Airflow webserver CSRF secret; set a random 32-char string |
| `POSTGRES_USER`           | No       | `postgres`     | PostgreSQL username                                     |
| `POSTGRES_PASSWORD`       | No       | `postgres`     | PostgreSQL password                                     |
| `POSTGRES_DB`             | No       | `ecommerce`    | PostgreSQL source database name                         |
