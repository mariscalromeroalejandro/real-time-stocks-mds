# Import requirements
import time
import json
import finnhub
import sys
from pathlib import Path
from kafka import KafkaProducer

sys.path.append(str(Path(__file__).resolve().parents[1]))
from market_hours import is_market_open, market_now, next_market_open, seconds_until_next_market_open
from settings import FINNHUB_API_KEY, KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC, SYMBOLS

# Setup client
finnhub_client = finnhub.Client(api_key=FINNHUB_API_KEY)

# Setup producer
producer = KafkaProducer(
    bootstrap_servers=[
        KAFKA_BOOTSTRAP_SERVERS # producer runs on host by default
    ],
    retries=5,
    acks="all",
    value_serializer=lambda x: json.dumps(x).encode("utf-8")
)

# Function to fetch data from finnhub
def fetch_stock_data(symbol):
    try:
        quote = finnhub_client.quote(symbol)
        quote["symbol"] = symbol
        quote["fetched_at"] = int(time.time())
        return quote
    except Exception as e:
        print(f"Error fetching data for {symbol}: {e}")
        return None

# Looping and Pushing to stream 
while True:
    now_et = market_now()
    if not is_market_open(now_et):
        wait_seconds = seconds_until_next_market_open(now_et)
        next_open_local = next_market_open(now_et)
        print(
            f"Market closed ({now_et.isoformat()}). "
            f"Sleeping {wait_seconds}s until next open ({next_open_local.isoformat()})."
        )
        time.sleep(wait_seconds)
        continue

    for s in SYMBOLS:
        quote = fetch_stock_data(s)
        if quote:
            print(f"Producing: {quote}")
            producer.send(topic=KAFKA_TOPIC, value=quote)
    time.sleep(6)
