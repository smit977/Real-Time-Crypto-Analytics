# Live Multi-Coin Crypto Dashboard with Kafka & Dash

## Overview
This project provides a **real-time multi-cryptocurrency dashboard** using Binance WebSocket streams, Kafka messaging, and Dash for interactive visualization. It allows you to:

- Stream live 1-minute candlestick data for multiple coins.
- Store data in **SQLite** for persistence.
- Detect **price & volume spikes** and anomalies.
- Visualize live candlestick charts, EMA, volume spikes, and anomalies in a **Dash dashboard**.

Coins tracked by default: **BTC, ETH, BNB, LTC**.

---

## Features
- **WebSocket Integration**: Connects to Binance WebSocket streams to fetch live market data.
- **Kafka Messaging**: Uses Kafka to reliably stream candlestick data.
- **SQLite Storage**: Saves coin-specific candle data for persistence.
- **Live Dash Visualization**:
  - Candlestick chart with **EMA indicator**.
  - Price and volume spike detection.
  - Anomaly detection based on range & volume z-scores.
  - Dynamic dropdown to select coins.
- **Multi-threaded Architecture**: Producer, consumer, and dashboard run independently in threads.

---

## Indicators & Thresholds:

- EMA period: EMA_PERIOD = 9
- Spike detection: PRICE_SPIKE_MULTIPLIER = 2.5, VOLUME_SPIKE_MULTIPLIER = 3.0
- Anomaly detection threshold: ANOMALY_THRESHOLD = 2.5
