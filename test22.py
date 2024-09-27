import json
import websocket
from kafka import KafkaProducer
import streamlit as st
import sqlite3
from pyspark.sql import SparkSession
from pyspark.sql.functions import window, col, avg
# Kafka producer setup
producer = KafkaProducer(bootstrap_servers="localhost:9092")

# SQLite3 setup
conn = sqlite3.connect('websocket_data.db')
cursor = conn.cursor()

# Create table if not exists
cursor.execute('''CREATE TABLE IF NOT EXISTS ticker_data (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    price REAL,
                    product_id TEXT,
                    time TEXT,
                    volume_24h REAL,
                    best_bid REAL,
                    best_ask REAL
                )''')
conn.commit()

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("WebSocket Data Processing") \
    .getOrCreate()
    
    
# WebSocket on_open callback
def on_open(ws):
    print("The socket is open")
    subscribe_message = {
        "type": "subscribe",
        "channels": [
            {"name": "ticker", "product_ids": ["BTC-USD", "ETH-USD", "LTC-USD"]}
        ],
    }
    ws.send(json.dumps(subscribe_message))

# WebSocket on_message callback
def on_message(ws, message):
    cur_data = json.loads(message)
    if cur_data["type"] == "ticker":
        # Insert data into database
        cursor.execute('''INSERT INTO ticker_data (price, product_id, time, volume_24h, best_bid, best_ask) 
                        VALUES (?, ?, ?, ?, ?, ?)''',
                        (cur_data["price"], cur_data["product_id"], cur_data["time"], cur_data["volume_24h"], cur_data["best_bid"], cur_data["best_ask"]))
        conn.commit()

        # Send data to appropriate Kafka topic
        topic = cur_data["product_id"]
        producer.send(
            topic,
            value=message.encode("utf-8"),
        )

        # Display data on Streamlit UI
        st.write("Price:", cur_data["price"],"Product ID:", cur_data["product_id"],"Time:", cur_data["time"],"24h Volume:", cur_data["volume_24h"],"Best Bid:", cur_data["best_bid"],"Best Ask:", cur_data["best_ask"])

# WebSocket URL
url = "wss://ws-feed.pro.coinbase.com"
ws = websocket.WebSocketApp(url, on_open=on_open, on_message=on_message)

# Run Streamlit app
if __name__ == "__main__":
    st.title("WebSocket Data Display")
    st.write("Connecting to WebSocket...")

    # Run WebSocket
    ws.run_forever()

