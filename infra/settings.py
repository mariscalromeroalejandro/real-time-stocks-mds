import os

from dotenv import load_dotenv


load_dotenv()


KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "stock-quotes")
KAFKA_CONSUMER_GROUP = os.getenv("KAFKA_CONSUMER_GROUP", "bronze-consumer")

FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY")
SYMBOLS = [symbol.strip() for symbol in os.getenv("SYMBOLS", "AAPL,GOOGL,MSFT,AMZN,TSLA").split(",") if symbol.strip()]

S3_ENDPOINT_URL = os.getenv("S3_ENDPOINT_URL", "http://localhost:9002")
BUCKET_NAME = os.getenv("BUCKET_NAME", "bronze-transactions")
MINIO_ROOT_USER = os.getenv("MINIO_ROOT_USER", "admin")
MINIO_ROOT_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD", "password123")
