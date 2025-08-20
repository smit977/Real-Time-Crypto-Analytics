import json
import ssl
import certifi
from datetime import datetime
import websocket
from kafka import KafkaProducer
import threading

# Kafka Configuration
KAFKA_BROKER = "localhost:9092"   

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# List of coins to track
COINS = ["btcusdt", "ethusdt", "bnbusdt", "ltcusdt"]  

# WebSocket Callbacks function
def on_message(ws, message):
    data = json.loads(message)
    kline = data["k"]
    coin = kline["s"].lower()  
    
    if kline["x"]:  # Only completed candles
        candle = {
            "coin": coin,
            "timestamp": datetime.fromtimestamp(kline["t"] / 1000).strftime("%Y-%m-%d %H:%M:%S"),
            "open": float(kline["o"]),
            "high": float(kline["h"]),
            "low": float(kline["l"]),
            "close": float(kline["c"]),
            "volume": float(kline["v"])
        }

        # Send to coin-specific Kafka topic
        topic_name = f"{coin}_candles_1m"  
        try:
            producer.send(topic_name, candle)  # Send single candle
            producer.flush()
            print(f"[{coin}] Candle sent to Kafka topic '{topic_name}': {candle}")
        except Exception as e:
            print(f"[{coin}] Error sending candle:", e)


def on_error(ws, error):
    print("Error:", error)


def on_close(ws, close_status_code, close_msg):
    print("WebSocket closed", close_status_code, close_msg)


def on_open(ws):
    print("WebSocket connection opened")


# Start WebSocket Threads per coin 
def start_ws_stream(coin):
    url = f"wss://fstream.binance.com/ws/{coin}@kline_1m"
    ws = websocket.WebSocketApp(
        url,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
        on_open=on_open
    )
    ws.run_forever(sslopt={"cert_reqs": ssl.CERT_REQUIRED, "ca_certs": certifi.where()})

# Start a thread for each coin
threads = []
for coin in COINS:
    t = threading.Thread(target=start_ws_stream, args=(coin,), daemon=True)
    t.start()
    threads.append(t)

# Keep main thread alive
for t in threads:
    t.join()
