import os
import boto3
import snowflake.connector
from dotenv import load_dotenv
from airflow import DAG
from airflow import settings
from airflow.exceptions import AirflowSkipException
from airflow.models import DagModel
from airflow.operators.python import PythonOperator
from datetime import datetime, time as dt_time, timedelta
from zoneinfo import ZoneInfo

load_dotenv()

MINIO_ENDPOINT_URL='http://minio:9000'
MINIO_ROOT_USER = os.getenv('MINIO_ROOT_USER', 'admin')
MINIO_ROOT_PASSWORD = os.getenv('MINIO_ROOT_PASSWORD', 'password123')
BUCKET_NAME = 'bronze-transactions'
LOCAL_DIR = '/tmp/minio_downloads'

SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DB = os.getenv("SNOWFLAKE_DB")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")
MARKET_TZ = ZoneInfo("America/New_York")
MARKET_OPEN = dt_time(hour=9, minute=30)
MARKET_CLOSE = dt_time(hour=16, minute=0)


def _is_market_open_now() -> bool:
    now_et = datetime.now(MARKET_TZ)
    if now_et.weekday() > 4:
        return False
    return MARKET_OPEN <= now_et.time() < MARKET_CLOSE


def pause_dag_when_market_closed(**kwargs):
    if _is_market_open_now():
        return

    dag_id = kwargs["dag"].dag_id
    session = settings.Session()
    try:
        dag_model = session.query(DagModel).filter(DagModel.dag_id == dag_id).one_or_none()
        if dag_model and not dag_model.is_paused:
            dag_model.set_is_paused(is_paused=True)
            session.commit()
    finally:
        session.close()

    raise AirflowSkipException("Market is closed. DAG has been paused.")

def download_from_minio():
    os.makedirs(LOCAL_DIR, exist_ok=True)
    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT_URL,
        aws_access_key_id=MINIO_ROOT_USER,
        aws_secret_access_key=MINIO_ROOT_PASSWORD
    )
    objects = s3.list_objects_v2(Bucket=BUCKET_NAME).get("Contents", [])
    local_files = []
    for obj in objects:
        key = obj["Key"]
        local_file = os.path.join(LOCAL_DIR, os.path.basename(key))
        s3.download_file(BUCKET_NAME, key, local_file)
        print(f"Downloaded {key} -> {local_file}")
        local_files.append(local_file)
    return local_files

def load_to_snowflake(**kwargs):
    local_files = kwargs['ti'].xcom_pull(task_ids='download_minio')
    if not local_files:
        print("No files to load.")
        return

    conn = snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DB,
        schema=SNOWFLAKE_SCHEMA
    )
    cur = conn.cursor()

    for f in local_files:
        cur.execute(f"PUT file://{f} @%bronze_stock_quotes_raw")
        print(f"Uploaded {f} to Snowflake stage")

    cur.execute("""
        COPY INTO bronze_stock_quotes_raw
        FROM @%bronze_stock_quotes_raw
        FILE_FORMAT = (TYPE=JSON)
    """)
    print("COPY INTO executed")

    cur.close()
    conn.close()

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2026, 2, 17),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "minio_to_snowflake",
    default_args=default_args,
    schedule_interval="*/1 * * * *",  # every 1 minutes
    catchup=False,
) as dag:
    market_hours_guard = PythonOperator(
        task_id="pause_if_market_closed",
        python_callable=pause_dag_when_market_closed,
        provide_context=True,
    )

    task1 = PythonOperator(
        task_id="download_minio",
        python_callable=download_from_minio,
    )

    task2 = PythonOperator(
        task_id="load_snowflake",
        python_callable=load_to_snowflake,
        provide_context=True,
    )

    market_hours_guard >> task1 >> task2
