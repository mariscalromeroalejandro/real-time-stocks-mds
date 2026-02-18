# Import requirements
import json
import boto3
import time
import sys
from pathlib import Path
from kafka import KafkaConsumer

sys.path.append(str(Path(__file__).resolve().parents[1]))
from settings import (
    BUCKET_NAME,
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_CONSUMER_GROUP,
    KAFKA_TOPIC,
    MINIO_ROOT_PASSWORD,
    MINIO_ROOT_USER,
    S3_ENDPOINT_URL,
)

# MinIO Connection 
s3 = boto3.client('s3', 
                  endpoint_url=S3_ENDPOINT_URL, 
                  aws_access_key_id=MINIO_ROOT_USER,
                  aws_secret_access_key=MINIO_ROOT_PASSWORD)

# Define Consumer
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=[
        KAFKA_BOOTSTRAP_SERVERS # consumer runs on host by default
    ],
    enable_auto_commit=True, # to save the offset after consuming messages
    group_id=KAFKA_CONSUMER_GROUP,
    value_deserializer=lambda x: json.loads(x.decode("utf-8")) # decode bytes to string and then parse JSON
)

print("Consumer is running and waiting for messages...")

# Main function
while True:
    message_pack = consumer.poll(timeout_ms=1000)
    for _, messages in message_pack.items():
        for message in messages:
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
