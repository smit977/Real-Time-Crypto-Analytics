import json
import sqlite3
from kafka import KafkaConsumer
import threading

# List of coin topics
TOPICS = ["btcusdt_candles_1m", "ethusdt_candles_1m", "bnbusdt_candles_1m", "ltcusdt_candles_1m"]

KAFKA_BROKER = 'localhost:9092'

def consume_and_store(topic_name):
    
    # Connect to coin-specific database
    db_name = topic_name.split("_")[0] + ".db"  
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    # Create table if not exists
    table_name = topic_name.split("_")[0] + "_candle" 
    cursor.execute(f'''
        CREATE TABLE IF NOT EXISTS {table_name} (
            timestamp TEXT PRIMARY KEY,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            volume REAL
        )
    ''')
    conn.commit()

    # Create Kafka consumer
    consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers=KAFKA_BROKER,
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),
        auto_offset_reset='earliest',
        enable_auto_commit=True
    )

    print(f"Started consuming from topic: {topic_name}")

    # Consume messages and insert into DB
    for message in consumer:
        candle = message.value  # single candle dict
        try:
            cursor.execute(f'''
                INSERT OR REPLACE INTO {table_name} (timestamp, open, high, low, close, volume)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (
                candle["timestamp"],
                candle["open"],
                candle["high"],
                candle["low"],
                candle["close"],
                candle["volume"]
            ))
            conn.commit()
            print(f"[{topic_name}] Inserted candle: {candle['timestamp']}")
        except Exception as e:
            print(f"[{topic_name}] Insert error:", e)

# Start a thread per topic
threads = []
for topic in TOPICS:
    t = threading.Thread(target=consume_and_store, args=(topic,), daemon=True)
    t.start()
    threads.append(t)

# Keep main thread alive
for t in threads:
    t.join()
