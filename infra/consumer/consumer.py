# Import requirements
import json
import boto3
import time
import os
from kafka import KafkaConsumer
from dotenv import load_dotenv


load_dotenv()

# Variables
S3_ENDPOINT_URL='http://localhost:9002'
BUCKET_NAME = 'bronze-transactions'
MINIO_ROOT_USER = os.getenv('MINIO_ROOT_USER', 'admin')
MINIO_ROOT_PASSWORD = os.getenv('MINIO_ROOT_PASSWORD', 'password123')
KAFKA_TOPIC = 'stock-quotes'

# MinIO Connection 
s3 = boto3.client('s3', 
                  endpoint_url=S3_ENDPOINT_URL, 
                  aws_access_key_id=MINIO_ROOT_USER,
                  aws_secret_access_key=MINIO_ROOT_PASSWORD)

# Define Consumer
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=[
        'localhost:29092' # consumer runs on host, outside docker
    ],
    enable_auto_commit=True, # to save the offset after consuming messages
    group_id="bronze-consumer",
    value_deserializer=lambda x: json.loads(x.decode("utf-8")) # decode bytes to string and then parse JSON
)

print("Consumer is running and waiting for messages...")

# Main function
for message in consumer:
    record = message.value
    symbol = record.get("symbol", "unknown")
    ts = record.get("fetched_at", int(time.time()))
    key = f"{symbol}/{ts}.json" #unique filename

    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=key,
        Body=json.dumps(record),
        ContentType='application/json'
    )
    print(f"Saved record for {symbol} = s3://{BUCKET_NAME}/{key}")