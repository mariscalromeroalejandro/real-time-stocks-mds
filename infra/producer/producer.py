# Import requirements
import time
import json
import finnhub
import os
from dotenv import load_dotenv
from kafka import KafkaProducer

load_dotenv()

# Define variables
FINNHUB_API_KEY=os.getenv('FINNHUB_API_KEY')
SYMBOLS = ['AAPL', 'GOOGL', 'MSFT', 'AMZN', 'TSLA']

# Setup client
finnhub_client = finnhub.Client(api_key=FINNHUB_API_KEY)

# Setup producer
producer = KafkaProducer(
    bootstrap_servers=[
        'localhost:29092' # producer runs on host, outside docker
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
    for s in SYMBOLS:
        quote = fetch_stock_data(s)
        if quote:
            print(f"Producing: {quote}")
            producer.send(topic="stock-quotes", value=quote)
    time.sleep(6)
