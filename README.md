# Real-Time Stocks MDS

## What this project is

This project is an end-to-end market data pipeline that ingests real-time stock quotes and moves them through a modern data stack:

- `Finnhub` for live stock quotes
- `Kafka` for streaming ingestion
- `MinIO` as S3-compatible object storage (bronze/raw files)
- `Airflow` for orchestration (`minio_to_snowflake` DAG)
- `Snowflake` for warehouse storage and transformations
- `dbt` for bronze/silver/gold modeling
- `Power BI` for analytics and visualization

Data flow:

1. Producer fetches stock quotes from Finnhub and publishes to Kafka topic `stock-quotes`.
2. Consumer reads Kafka messages and uploads JSON payloads to MinIO bucket.
3. Airflow DAG loads files from MinIO into Snowflake raw table.
4. dbt transforms raw data into bronze, silver, and gold layers.
5. Power BI connects to Snowflake for dashboards.

## Project setup

### 1. Environment variables

Copy `.env.example` to `.env` and fill your real credentials:

```powershell
Copy-Item .env.example .env
```

Set at least:

- `FINNHUB_API_KEY`
- `MINIO_ROOT_USER`
- `MINIO_ROOT_PASSWORD`
- `SNOWFLAKE_USER`
- `SNOWFLAKE_PASSWORD`
- `SNOWFLAKE_ACCOUNT`
- `SNOWFLAKE_WAREHOUSE`
- `SNOWFLAKE_DB`
- `SNOWFLAKE_SCHEMA`

## Run infrastructure

From the `infra` folder:

```powershell
docker compose up -d
```

Initialize Airflow metadata DB:

```powershell
docker compose exec airflow-scheduler airflow db init
docker compose exec airflow-webserver airflow db init
```

Create Airflow admin user (replace placeholders):

```powershell
docker compose exec airflow-webserver airflow users create --username xxxx --firstname xxxx --lastname xxxx --role Admin --email xxxx@xxxx.com --password xxxx
```

## Service URLs

- Kafdrop: http://localhost:9000/
- MinIO Console: http://localhost:9001/
- Airflow Webserver: http://localhost:8080/

Use your configured credentials to log in.

## Finnhub setup

1. Register at https://finnhub.io/
2. Create an API key
3. Put the key in `.env` as `FINNHUB_API_KEY`

## Kafka setup

Create topic `stock-quotes`:

- Partitions: `3`
- Replication factor: `1`

Example:

```powershell
docker compose exec kafka kafka-topics --create --topic stock-quotes --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
```

## Producer setup

1. Ensure `requirements.txt` is present
2. Activate virtual environment
3. Install dependencies
4. Run producer

```powershell
.\venv\Scripts\Activate.ps1
pip install -r requirements.txt
python infra/producer/producer.py
```

## Consumer setup

Implement/run consumer that:

1. Connects to MinIO via `boto3`
2. Creates `KafkaConsumer`
3. Reads messages and uploads to MinIO bucket

Then run:

```powershell
python infra/consumer/consumer.py
```

## MinIO bucket

Create bucket used for raw ingestion (example: `bronze-transactions`) in MinIO Console.

## Snowflake setup

Create:

- Database: `STOCKS_MDS`
- Raw table: `BRONZE_STOCK_QUOTES_RAW`

## Airflow DAG

Place `minio_to_snowflake` DAG code in your Airflow DAGs folder (for this compose setup: `infra/dags/`).

- DAG appears automatically in Airflow UI
- Trigger the DAG
- Verify Snowflake table receives data

## dbt setup

1. Create/init project folder:

```powershell
mkdir dbt_stocks
cd dbt_stocks
dbt init dbt_stocks
```

2. Create model layers:

- `bronze/sources.yml`
- `bronze/bronze_stg_stock_quotes.sql`
- `silver/silver_clean_stock_quotes.sql`
- `gold/gold_kpi.sql`
- `gold/gold_candlestick.sql`
- `gold/gold_treechart.sql`

3. Run dbt:

```powershell
dbt run
dbt test
```

## Power BI

Connect Microsoft Power BI to Snowflake and build reports from gold models.

## Typical execution order

1. Start docker services
2. Init Airflow DB and create user
3. Configure `.env`
4. Create Kafka topic and MinIO bucket
5. Run producer and consumer
6. Run Airflow DAG to load Snowflake
7. Run dbt models
8. Connect Power BI
