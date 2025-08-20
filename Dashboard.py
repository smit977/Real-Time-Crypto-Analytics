import pandas as pd
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import threading
import json
from kafka import KafkaConsumer

# Configuration 
COINS = ["btcusdt", "ethusdt", "bnbusdt", "ltcusdt"]
KAFKA_TOPICS = [f"{coin}_candles_1m" for coin in COINS]
KAFKA_BROKER = 'localhost:9092'

# Chart & Analysis Settings
EMA_PERIOD = 9
SPIKE_WINDOW = 20
PRICE_SPIKE_MULTIPLIER = 2.5
VOLUME_SPIKE_MULTIPLIER = 3.0
ANOMALY_DETECTION_WINDOW = 50
ANOMALY_THRESHOLD = 2.5
MAX_DF_SIZE = 60    

# Global Shared Objects for Thread-Safe Data Handling 
SHARED_DATA_LOCK = threading.Lock()
SHARED_DATA = {coin: pd.DataFrame() for coin in COINS}

# Kafka Consumer  
def kafka_consumer_thread():
    #Consumes data from multiple Kafka topics and updates the shared data dictionary.
    global SHARED_DATA
    consumer = KafkaConsumer(
        *KAFKA_TOPICS, # Subscribe to all topics at once
        bootstrap_servers=KAFKA_BROKER,
        auto_offset_reset='earliest',
        value_deserializer=lambda v: json.loads(v.decode('utf-8'))
    )
    print("Kafka consumer thread started for all topics.")

    for message in consumer:
        coin_name = message.topic.split("_")[0]
        
        candle_batch = message.value
        if not isinstance(candle_batch, list):
            candle_batch = [candle_batch]

        new_data = pd.DataFrame(candle_batch)
        new_data['timestamp'] = pd.to_datetime(new_data['timestamp'])
        new_data = new_data.set_index('timestamp')
        
        with SHARED_DATA_LOCK:
            coin_df = SHARED_DATA[coin_name]
            if coin_df.empty:
                SHARED_DATA[coin_name] = new_data
            else:
                SHARED_DATA[coin_name] = pd.concat([coin_df, new_data])
            
            # Keep the DataFrame from growing indefinitely
            SHARED_DATA[coin_name] = SHARED_DATA[coin_name][
                ~SHARED_DATA[coin_name].index.duplicated(keep='last')
            ].sort_index().tail(MAX_DF_SIZE)

#Dash Web Application
app = dash.Dash(__name__)

app.layout = html.Div(style={'backgroundColor': '#111111', 'color': '#FFFFFF'}, children=[
    html.H1("Live Multi-Coin Dashboard", style={'textAlign': 'center'}),
    
    html.Div([
        html.Label("Select Coin:", style={'marginRight': '10px'}),
        dcc.Dropdown(
            id='coin-dropdown',
            options=[{'label': coin.upper(), 'value': coin} for coin in COINS],
            value='btcusdt', # Default coin to display
            style={'width': '200px', 'color': '#000'}
        ),
    ], style={'textAlign': 'center', 'padding': '10px'}),
    
    dcc.Graph(id='live-graph'),
    
    dcc.Interval(
        id='interval-component',
        interval=60 * 1000, 
        n_intervals=0
    )
])

#  Main Callback to Update the Graph 
@app.callback(
    Output('live-graph', 'figure'),
    Input('interval-component', 'n_intervals'),
    Input('coin-dropdown', 'value')
)
def update_graph(n, selected_coin):
    """Triggered by the interval or dropdown to update the chart."""
    if not selected_coin:
        return go.Figure(layout={"template": "plotly_dark", "title": "Please select a coin"})

    # Read Data from the In-Memory Store 
    with SHARED_DATA_LOCK:
        if SHARED_DATA[selected_coin].empty:
            return go.Figure(layout={"template": "plotly_dark", "title": f"Waiting for data for {selected_coin.upper()}..."})
        
        # Use the entire DataFrame, not just the tail
        plot_df = SHARED_DATA[selected_coin].copy()

    # Calculate Indicators, Spikes, and Anomalies 
    plot_df['ema_9'] = plot_df['close'].ewm(span=EMA_PERIOD, adjust=False).mean()
    plot_df['range'] = plot_df['high'] - plot_df['low']
    
    range_ma = plot_df['range'].rolling(window=SPIKE_WINDOW).mean()
    volume_ma = plot_df['volume'].rolling(window=SPIKE_WINDOW).mean()
    price_spikes = plot_df[plot_df['range'] > range_ma * PRICE_SPIKE_MULTIPLIER]
    volume_spikes = plot_df[plot_df['volume'] > volume_ma * VOLUME_SPIKE_MULTIPLIER]

    range_std = plot_df['range'].rolling(window=ANOMALY_DETECTION_WINDOW).std()
    volume_std = plot_df['volume'].rolling(window=ANOMALY_DETECTION_WINDOW).std()
    
    z_score_range = (plot_df['range'] - range_ma) / range_std
    z_score_volume = (plot_df['volume'] - volume_ma) / volume_std
    
    plot_df['anomaly_score'] = (z_score_range.abs() + z_score_volume.abs()) / 2
    anomalies = plot_df[plot_df['anomaly_score'] > ANOMALY_THRESHOLD]

# Build the Figure 
    fig = make_subplots(
        rows=3, cols=1, row_heights=[0.7, 0.15, 0.15], shared_xaxes=True, vertical_spacing=0.02,
        subplot_titles=(f'{selected_coin.upper()} Price', 'Volume', 'Anomaly Score')
    )
    fig.add_trace(go.Candlestick(x=plot_df.index, name='Price', open=plot_df['open'], high=plot_df['high'], low=plot_df['low'], close=plot_df['close'], increasing_line_color='#26a69a', decreasing_line_color='#ef5350'), row=1, col=1)
    fig.add_trace(go.Scatter(x=plot_df.index, y=plot_df['ema_9'], name='9 EMA', line=dict(color='yellow', width=1)), row=1, col=1)
    fig.add_trace(go.Scatter(x=price_spikes.index, y=price_spikes['high'] * 1.005, name='Price Spike', mode='markers', marker=dict(symbol='triangle-up', color='orange', size=12)), row=1, col=1)
    fig.add_trace(go.Scatter(x=volume_spikes.index, y=volume_spikes['low'] * 0.995, name='Volume Spike', mode='markers', marker=dict(symbol='star', color='purple', size=10)), row=1, col=1)
    
    if not anomalies.empty:
        fig.add_trace(go.Scatter(x=anomalies.index, y=anomalies['high'], name='Anomaly', mode='markers', marker=dict(symbol='circle-open', color='red', size=15, line=dict(width=3))), row=1, col=1)
    
    volume_colors = np.where(plot_df.index.isin(volume_spikes.index), 'purple', 'rgba(128,128,128,0.5)')
    fig.add_trace(go.Bar(x=plot_df.index, y=plot_df['volume'], name="Volume", marker_color=volume_colors), row=2, col=1)
    
    anomaly_colors = np.where(plot_df['anomaly_score'] > ANOMALY_THRESHOLD, 'red', 'dimgray')
    fig.add_trace(go.Bar(x=plot_df.index, y=plot_df['anomaly_score'], name="Anomaly Score", marker_color=anomaly_colors), row=3, col=1)
    
    fig.add_hline(y=ANOMALY_THRESHOLD, line_width=1, line_dash="dash", line_color="red", row=3, col=1)
    
    fig.update_layout(template='plotly_dark', xaxis_rangeslider_visible=False, height=800, showlegend=False, margin=dict(l=40, r=40, t=40, b=20))
    fig.update_yaxes(title_text="Price (USDT)", row=1, col=1)
    
    return fig

#  Main Execution Block 
if __name__ == '__main__':
    # Start the Kafka consumer in a background thread
    consumer_thread = threading.Thread(target=kafka_consumer_thread, daemon=True)
    consumer_thread.start()
    
    print("Starting Dash server on http://127.0.0.1:8050/")
    app.run(debug=False)